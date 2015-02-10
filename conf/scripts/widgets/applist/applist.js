/* *
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
'use strict';

angular.module('app.widgets.applist', ['adf.provider'])
.value('appMastersUrl', location.origin+'/appmasters')
.config(function(dashboardProvider){
  dashboardProvider.widget('applist', {
    title: 'Applications',
    description: 'Displays a list of applications',
    controller: 'applistCtrl',
    controllerAs: 'list',
    templateUrl: 'scripts/widgets/applist/applist.html',
    edit: {
      templateUrl: 'scripts/widgets/applist/edit.html',
      reload: false,
      controller: 'applistEditCtrl'
    }
  });
})
.controller('applistCtrl', function($scope, $http, config, appMastersUrl){
  function getAppList(response){
    if (!config.appMasters){
      config.appMasters = [];
    }
    var data = (response.data && response.data.appMasters) || [];
    data.forEach(function(appMaster) {
      config.appMasters.push(appMaster);
    });
    this.appMasters = config.appMasters;
  }
  var _getAppList = getAppList.bind(this);
  $http.get(appMastersUrl).then(_getAppList, function(err){
    throw err; 
  });
}).controller('applistEditCtrl', function($scope){
    function getLinks(){
      if (!$scope.config.appMasters){
        $scope.config.appMasters = [];
      }
      return $scope.config.appMasters;
    }
    $scope.addLink = function(){
      getLinks().push({});
    };
    $scope.removeLink = function(index){
      getLinks().splice(index, 1);
    };
});
