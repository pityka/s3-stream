package com.bluelabs.akkaaws

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}

// Documentation: http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
case class CanonicalRequest(method: String,
                            uri: String,
                            queryString: String,
                            headerString: String,
                            signedHeaders: String,
                            hashedPayload: String) {
  def canonicalString: String = {
    s"$method\n$uri\n$queryString\n$headerString\n\n$signedHeaders\n$hashedPayload"
  }
}

object CanonicalRequest {
  def from(req: HttpRequest): CanonicalRequest = {
    val hashedBody = req.headers
      .find(_.name == "x-amz-content-sha256")
      .map(_.value())
      .getOrElse("")
    CanonicalRequest(req.method.value,
                     req.uri.path.toString(),
                     canonicalQueryString(req.uri.query()),
                     canonicalHeaderString(req.headers),
                     signedHeadersString(req.headers),
                     hashedBody)
  }

  def canonicalQueryString(query: Query): String = {
    query
      .sortBy(_._1)
      .map { case (a, b) => s"${uriEncode(a)}=${uriEncode(b)}" }
      .mkString("&")
  }

  private def uriEncode(str: String) = {
    java.net.URLEncoder.encode(str, "utf-8")
  }

  def canonicalHeaderString(headers: Seq[HttpHeader]): String = {
    val grouped: Map[String, Seq[HttpHeader]] =
      headers.groupBy(_.lowercaseName())
    val combined = grouped.mapValues(
      _.map(_.value().replaceAll("\\s+", " ").trim).mkString(","))
    combined.toList
      .sortBy(_._1)
      .map { case (k: String, v: String) => s"$k:$v" }
      .mkString("\n")
  }

  def signedHeadersString(headers: Seq[HttpHeader]): String = {
    headers.map(_.lowercaseName()).distinct.sorted.mkString(";")
  }

}
