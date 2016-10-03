/*
 * The MIT License
 *
 * Copyright (c) 2013, Sebastian Sdorra
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

angular.module('app', [
  'adf', 
  'app.widgets.news', 
  'app.widgets.randommsg',
  'app.widgets.weather', 
  'app.widgets.markdown',
  'app.widgets.linklist', 
  'app.widgets.github',
  'app.widgets.version', 
  'app.widgets.dag', 
  'LocalStorageModule',
  'structures', 
  'app-01', 
  'ngRoute'
])
.config(function($routeProvider, localStorageServiceProvider){
  localStorageServiceProvider.setPrefix('adf');

  $routeProvider.when('/app/01', {
    templateUrl: 'partials/app.html',
    controller: 'app01Ctrl'
  })
  .otherwise({
    redirectTo: '/app/01'
  });

})
.controller('navigationCtrl', function($scope, $location){

  $scope.navCollapsed = true;

  $scope.toggleNav = function(){
    $scope.navCollapsed = !$scope.navCollapsed;
  };

  $scope.$on('$routeChangeStart', function() {
    $scope.navCollapsed = true;
  });

  $scope.navClass = function(page) {
    var currentRoute = $location.path().substring(1) || 'Applications';
    return page === currentRoute || new RegExp(page).test(currentRoute) ? 'active' : '';
  };

});
