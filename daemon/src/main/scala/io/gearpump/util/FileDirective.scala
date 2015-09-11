/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.gearpump.util


import java.io.File

import akka.http.scaladsl.model.{HttpEntity, MediaTypes, Multipart}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.Materializer
import akka.stream.io.{SynchronousFileSink, SynchronousFileSource}

import scala.concurrent.{ExecutionContext, Future}


object FileDirective {

  //form field name
  type Name = String

  case class FileInfo(originFileName: String, file: File, length: Long)

  private def uploadFileImpl(rootDirectory: File)(implicit mat: Materializer, ec: ExecutionContext): Directive1[Future[Map[Name, FileInfo]]] = {
    Directive[Tuple1[Future[Map[Name, FileInfo]]]] { inner =>
      entity(as[Multipart.FormData]) { (formdata: Multipart.FormData) =>
        val fileNameMap = formdata.parts.mapAsync(1) { p =>
          if (p.filename.isDefined) {
            val targetPath = File.createTempFile(s"userfile_${p.name}_${p.filename.getOrElse("")}", "", rootDirectory)
            val written = p.entity.dataBytes.runWith(SynchronousFileSink(targetPath))
            written.map(written =>
              Map(p.name -> FileInfo(p.filename.get, targetPath, written)))
          } else {
            Future(Map.empty[Name, FileInfo])
          }
        }.runFold(Map.empty[Name, FileInfo])((set, value) => set ++ value)
        inner(Tuple1(fileNameMap))
      }
    }
  }

  def uploadFile: Directive1[Map[Name, FileInfo]] = {
    uploadFileTo(null)
  }

  def uploadFileTo(rootDirectory: File): Directive1[Map[Name, FileInfo]] = {
    Directive[Tuple1[Map[Name, FileInfo]]] { inner =>
      extractMaterializer {implicit mat =>
        extractExecutionContext {implicit ec =>
          uploadFileImpl(rootDirectory)(mat, ec) { filesFuture =>
            ctx => {
              filesFuture.map(map => inner(Tuple1(map))).flatMap(route => route(ctx))
            }
          }
        }
      }
    }
  }

  def downloadFile(file: File): Route = {
    val responseEntity = HttpEntity(
      MediaTypes.`application/octet-stream`,
      file.length,
      SynchronousFileSource(file, chunkSize = 262144))
    complete(responseEntity)
  }
}