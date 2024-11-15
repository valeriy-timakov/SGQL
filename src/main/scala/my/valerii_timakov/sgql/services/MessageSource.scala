package my.valerii_timakov.sgql.services

import akka.http.scaladsl.model.headers.Language

import scala.util.Try


trait MessageSource:
    def getMessage(mnemo: String, language: Language): String
    
class MessageSourceImpl extends MessageSource:
    override def getMessage(mnemo: String, language: Language): String = mnemo