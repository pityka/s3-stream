package com.bluelabs.s3stream.impl

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import com.bluelabs.akkaaws.{Signer}
import com.bluelabs.s3stream._

import akka.NotUsed
import akka.event.Logging
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.{Attributes}
import akka.stream.scaladsl.{Flow, Sink, Keep}
import akka.util.ByteString
import Marshalling.{
  MultipartUploadUnmarshaller,
  completeMultipartUploadResultUnmarshaller
}

private[s3stream] trait MultipartUploadSupport
    extends MultipartUploadHttpRequests
    with SignAndGet {

  protected def retryFuture[A](f: => Future[A], c: Int): Future[A] =
    if (c > 0) f.recoverWith { case e =>
      akka.pattern.after(2 seconds, system.scheduler)(retryFuture(f, c - 1))
    }
    else f

  protected def retryRequest(h: HttpRequest, c: Int) =
    retryFuture(singleRequest(h), c)

  /** Uploades a stream of ByteStrings to a specified location as a multipart
    * upload.
    *
    * @param s3Location
    * @param chunkSize
    * @param chunkingParallelism
    * @return
    */
  def multipartUpload(
      s3Location: S3Location,
      chunkSize: Int = MIN_CHUNK_SIZE,
      chunkingParallelism: Int = 4,
      params: PostObjectRequest = PostObjectRequest.default
  ): Sink[ByteString, Future[CompleteMultipartUploadResult]] = {
    val mp = retryFuture(initiateMultipartUpload(s3Location, params), 4)

    val requestFlow =
      createRequestFlow(mp, chunkSize, chunkingParallelism)
    val responseFlow =
      createResponseFlow(requestFlow)

    responseFlow
      .log("s3-upload-response")
      .withAttributes(
        Attributes.logLevels(
          onElement = Logging.DebugLevel,
          onFailure = Logging.WarningLevel,
          onFinish = Logging.DebugLevel
        )
      )
      .toMat(completionSink(s3Location, mp))(Keep.right)
  }

  protected def initiateMultipartUpload(
      s3Location: S3Location,
      params: PostObjectRequest
  ): Future[MultipartUpload] = {

    val req = initiateMultipartUploadRequest(s3Location, params)
    val response = for {
      signingKey <- signingKey
      signedReq <- Signer.signedRequest(req, signingKey)
      response <- singleRequest(signedReq)
    } yield {
      response
    }
    response.flatMap {
      case HttpResponse(status, _, entity, _) if status.isSuccess() =>
        Unmarshal(entity).to[MultipartUpload]
      case HttpResponse(status, _, entity, _) => {
        Unmarshal(entity).to[String].flatMap { case err =>
          Future.failed(new Exception(err))
        }
      }
      case _ => Future.failed(new RuntimeException())
    }
  }

  protected def completeMultipartUpload(
      s3Location: S3Location,
      parts: Seq[SuccessfulUploadPart]
  ): Future[CompleteMultipartUploadResult] =
    for {
      req <- completeMultipartUploadRequest(
        parts.head.multipartUpload,
        parts.map(p => p.index -> p.etag)
      )
      res <- signAndGetAs[CompleteMultipartUploadResult](req)
    } yield res

  /** Transforms a flow of ByteStrings into a flow of HTTPRequests to upload to
    * S3.
    *
    * @param s3Location
    * @param chunkSize
    * @param parallelism
    * @return
    */
  protected def createRequestFlow(
      f: Future[MultipartUpload],
      chunkSize: Int = MIN_CHUNK_SIZE,
      parallelism: Int = 4
  ): Flow[ByteString, (HttpRequest, (MultipartUpload, Int)), NotUsed] = {
    assert(
      chunkSize >= MIN_CHUNK_SIZE,
      "Chunk size must be at least 5242880B. See http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html"
    )

    Flow[ByteString]
      .via(new Chunker(chunkSize))
      .zipWith(makeCounterSource(f)) {
        case (payload, (uploadInfo, chunkIndex)) =>
          (
            uploadPartRequest(uploadInfo, chunkIndex, payload),
            (uploadInfo, chunkIndex)
          )
      }
      .mapAsync(parallelism) { case (req, info) =>
        signingKey.flatMap(signingKey =>
          Signer.signedRequest(req, signingKey).zip(Future.successful(info))
        )
      }
  }

  protected def createResponseFlow(
      requestFlow: Flow[
        ByteString,
        (HttpRequest, (MultipartUpload, Int)),
        NotUsed
      ]
  ): Flow[ByteString, UploadPartResponse, NotUsed] =
    requestFlow
      .mapAsync(4)(rq =>
        retryRequest(rq._1, 4).map(x => Success(x) -> rq._2).recover { case e =>
          Failure(e) -> rq._2
        }
      )
      .map {
        case (Success(r), (upload, index)) => {
          r.discardEntityBytes()
          val etag = r.headers.find(_.lowercaseName() == "etag").map(_.value())
          etag
            .map(t => SuccessfulUploadPart(upload, index, t))
            .getOrElse(
              FailedUploadPart(
                upload,
                index,
                new RuntimeException("Cannot find etag")
              )
            )
        }
        case (Failure(e), (upload, index)) => FailedUploadPart(upload, index, e)
      }

  protected def completionSink(
      s3Location: S3Location,
      mp: Future[MultipartUpload]
  ): Sink[UploadPartResponse, Future[CompleteMultipartUploadResult]] =
    Sink.seq[UploadPartResponse].mapMaterializedValue {
      case responseFuture: Future[Seq[UploadPartResponse]] =>
        responseFuture
          .flatMap { case responses: Seq[UploadPartResponse] =>
            val successes = responses.collect { case r: SuccessfulUploadPart =>
              r
            }
            val failures = responses.collect { case r: FailedUploadPart => r }
            if (responses.isEmpty) {
              Future.failed(new RuntimeException("No Responses"))
            } else if (failures.isEmpty) {
              Future.successful(successes.sortBy(_.index))
            } else {
              Future.failed(FailedUpload(failures.map(_.exception)))
            }
          }
          .flatMap(completeMultipartUpload(s3Location, _))
          .recoverWith { case e =>
            akka.event
              .Logging(system.eventStream, "s3-stream")
              .error(e, "Failed multipart upload.")
            mp.foreach { case MultipartUpload(_, id) =>
              val req =
                abortMultipartUploadRequest(s3Location, id)
              signAndGet(req).map { response =>
                response.dataBytes.runWith(Sink.ignore);
                response
              }
            }
            Future.failed(e)
          }
    }

}
