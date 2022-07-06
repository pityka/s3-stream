package com.bluelabs.akkaaws.impl

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import scala.collection.compat._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers

// Documentation: http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
private[akkaaws] case class CanonicalRequest(
    method: String,
    uri: String,
    queryString: String,
    headerString: String,
    signedHeaders: String,
    hashedPayload: String
) {
  def canonicalString: String = {
    s"$method\n$uri\n$queryString\n$headerString\n\n$signedHeaders\n$hashedPayload"
  }
}

private[akkaaws] object CanonicalRequest {

  // this list of headers are copied from Alpakka
  // Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
  // http://www.apache.org/licenses/LICENSE-2.0
  private val akkaSyntheticHeaderNames = List(
    headers.`Raw-Request-URI`.lowercaseName,
    headers.`X-Forwarded-For`.lowercaseName,
    headers.`Timeout-Access`.lowercaseName,
    headers.`Tls-Session-Info`.lowercaseName
  )

  def from(req: HttpRequest): CanonicalRequest = {
    val hashedBody = req.headers
      .find(_.name == "x-amz-content-sha256")
      .map(_.value())
      .getOrElse("")

    val signedHeaders = req.headers.filterNot(header =>
      akkaSyntheticHeaderNames.contains(header.lowercaseName())
    )

    CanonicalRequest(
      req.method.value,
      encode(req.uri.path),
      canonicalQueryString(req.uri.query()),
      canonicalHeaderString(signedHeaders),
      signedHeadersString(signedHeaders),
      hashedBody
    )
  }

  def canonicalQueryString(query: Query): String = {
    def uriEncode(s: String): String = s.flatMap {
      case c if isUnreservedCharacter(c) => c.toString
      case c                             => "%" + c.toHexString.toUpperCase
    }

    query
      .sortBy { case (name, _) => name }
      .map { case (name, value) => s"${uriEncode(name)}=${uriEncode(value)}" }
      .mkString("&")
  }

  def canonicalHeaderString(headers: Seq[HttpHeader]): String = {
    val grouped: Map[String, Seq[HttpHeader]] =
      headers.groupBy(_.lowercaseName())
    val combined = grouped.view.mapValues(
      _.map(_.value().replaceAll("\\s+", " ").trim).mkString(",")
    )
    combined.toList
      .sortBy(_._1)
      .map { case (k: String, v: String) => s"$k:$v" }
      .mkString("\n")
  }

  def signedHeadersString(headers: Seq[HttpHeader]): String = {
    headers.map(_.lowercaseName()).distinct.sorted.mkString(";")
  }

  // the below 4 methods are copied from Alpakka
  // Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
  // http://www.apache.org/licenses/LICENSE-2.0

  // https://tools.ietf.org/html/rfc3986#section-2.3
  def isUnreservedCharacter(c: Char): Boolean =
    c.isLetterOrDigit || c == '-' || c == '.' || c == '_' || c == '~'

  // https://tools.ietf.org/html/rfc3986#section-2.2
  // Excludes "/" as it is an exception according to spec.
  val reservedCharacters: String = ":?#[]@!$&'()*+,;="

  def isReservedCharacter(c: Char): Boolean =
    reservedCharacters.contains(c)

  def encode(path: Uri.Path): String =
    if (path.isEmpty) "/"
    else {
      path.toString.flatMap {
        case c if isReservedCharacter(c) => "%" + c.toHexString.toUpperCase
        case c                           => c.toString
      }
    }

}
