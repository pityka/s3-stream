package com.bluelabs.akkaaws

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Host, RawHeader}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.{AnyFlatSpecLike => FlatSpecLike}

class SignerSpec(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with ScalaFutures {
  def this() = this(ActorSystem("SignerSpec"))

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

  @scala.annotation.nowarn
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withDebugLogging(true)
  )

  val credentials =
    impl.CredentialImpl.BasicCredentials(
      "AKIDEXAMPLE",
      "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
    )
  val scope = CredentialScope(LocalDate.of(2015, 8, 30), "us-east-1", "iam")
  val signingKey = impl.SigningKey(credentials, scope)
  val signingKeyProvider = SigningKeyProvider.static(
    "AKIDEXAMPLE",
    "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
    "us-east-1"
  )

  val cr = impl.CanonicalRequest(
    "GET",
    "/",
    "Action=ListUsers&Version=2010-05-08",
    "content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:iam.amazonaws.com\nx-amz-date:20150830T123600Z",
    "content-type;host;x-amz-date",
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  )

  "Signer" should "calculate the string to sign" in {
    val date = LocalDateTime.of(2015, 8, 30, 12, 36, 0).atZone(ZoneOffset.UTC)
    val stringToSign: String =
      Signer.stringToSign("AWS4-HMAC-SHA256", signingKey, date, cr)
    stringToSign should equal(
      "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\nf536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
    )
  }

  ignore should "add the date, content hash, and authorization headers to a request" in {
    val req = HttpRequest(HttpMethods.GET)
      .withUri("https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08")
      .withHeaders(
        Host("iam.amazonaws.com"),
        RawHeader(
          "Content-Type",
          "application/x-www-form-urlencoded; charset=utf-8"
        )
      )

    val srFuture = Signer.signedRequest(
      req,
      signingKeyProvider,
      LocalDateTime.of(2015, 8, 30, 12, 36, 0).atZone(ZoneOffset.UTC)
    )
    whenReady(srFuture) { case signedRequest =>
      signedRequest should equal(
        HttpRequest(HttpMethods.GET)
          .withUri(
            "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08"
          )
          .withHeaders(
            Host("iam.amazonaws.com"),
            RawHeader(
              "Content-Type",
              "application/x-www-form-urlencoded; charset=utf-8"
            ),
            RawHeader("x-amz-date", "20150830T123600Z"),
            RawHeader(
              "x-amz-content-sha256",
              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            ),
            RawHeader(
              "Authorization",
              "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=dd479fa8a80364edf2119ec24bebde66712ee9c9cb2b0d92eb3ab9ccdc0c3947"
            )
          )
      )
    }
  }

}
