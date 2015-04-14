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
package org.apache.gearpump.examples.transport.generator

import scala.util.Random
import MockCity._

class MockCity(size: Int) {
  private val random = new Random()
  private val directions = Array(UP, DOWN, LEFT, RIGHT)

  def nextLocation(currentLocationId: String): String = {
    val coordinate = idToCoordinate(currentLocationId)
    val direction = directions(random.nextInt(4))
    val newCoordinate = coordinate.addOffset(direction)
    if(inCity(newCoordinate)) {
      coordinateToId(newCoordinate)
    } else {
      nextLocation(currentLocationId)
    }
  }
  
  def getDistance(locationId1: String, locationId2: String): Long = {
    val coordinate1 = idToCoordinate(locationId1)
    val coordinate2 = idToCoordinate(locationId2)
    val blocks = Math.abs(coordinate1.row - coordinate2.row) + Math.abs(coordinate1.column - coordinate2.column)
    blocks * LENGTH_PER_BLOCK
  }
  
  def randomLocationId(): String = {
    val row = random.nextInt(size)
    val column = random.nextInt(size)
    coordinateToId(Coordinate(row, column))
  }
  
  private def coordinateToId(coordinate: Coordinate): String = {
    s"Id_${coordinate.row}_${coordinate.column}"
  }
  
  private def idToCoordinate(locationId: String): Coordinate = {
    val attr = locationId.split("_")
    val row = attr(1).toInt
    val column =attr(2).toInt
    Coordinate(row, column)
  }

  private def inCity(coordinate: Coordinate): Boolean = {
    coordinate.row >= 0 &&
      coordinate.row < size &&
      coordinate.column >= 0 &&
      coordinate.column < size
  }
}

object MockCity{
  //The length of the mock city, km
  final val LENGTH_PER_BLOCK = 5
  //The minimal speed, km/h
  final val MINIMAL_SPEED = 10
  
  final val UP = Coordinate(0, 1)
  final val DOWN = Coordinate(0, -1)
  final val LEFT = Coordinate(-1, 0)
  final val RIGHT = Coordinate(1, 0)
  
  case class Coordinate(row: Int, column: Int) {
    def addOffset(coordinate: Coordinate): Coordinate = {
      Coordinate(this.row + coordinate.row, this.column + coordinate.column)
    }
  }
}
