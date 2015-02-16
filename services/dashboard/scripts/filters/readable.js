/**
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
'use strict';

angular.module('readable', [])
.filter('readableTime', function() {
  return function (millis) {
    var millisToPeriod = function(millis) {
      var units = [
        {label: "millis", mod: 1000},
        {label: "seconds", mod: 60},
        {label: "minutes", mod: 60},
        {label: "hours", mod: 24},
        {label: "days", mod: 7},
        {label: "weeks", mod: 52},
      ];
      var duration = {};
      var x = millis;
      for (var i = 0; i < units.length; i++) {
        var t = x % units[i].mod;
        duration[units[i].label] = t;
        x = (x - t) / units[i].mod
      }
      return duration;
    }
    var checkAndAdd = function(array, value, unit) {
      if (value > 0) {
        array.push(value + " " + unit);
      }
    }

    var period = millisToPeriod(parseInt(millis, 10));
    var parts = [];
    checkAndAdd(parts, period.weeks, "weeks");
    checkAndAdd(parts, period.days, "days");
    checkAndAdd(parts, period.hours, "hours");
    checkAndAdd(parts, period.minutes, "mins");
    checkAndAdd(parts, period.seconds, "secs");
    if (parts.length == 0) {
        return period.millis > 0 ? period.millis + "ms" : "N/A";
    }
    return parts.slice(0, 2).join(" and ");
  };
});