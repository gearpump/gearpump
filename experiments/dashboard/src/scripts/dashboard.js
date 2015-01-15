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

/**
 * @ngdoc directive
 * @name adf.directive:adfDashboard
 * @element div
 * @restrict EA
 * @scope
 * @description
 *
 * `adfDashboard` is a directive which renders the dashboard with all its
 * components. The directive requires a name attribute. The name of the
 * dashboard can be used to store the model.
 */


'use strict';

angular.module('adf')
  .directive('adfDashboard', function($rootScope, $log, $modal, dashboard, adfTemplatePath){

    function copyWidgets(source, target){
      if ( source.widgets && source.widgets.length > 0 ){
        var w = source.widgets.shift();
        while (w){
          target.widgets.push(w);
          w = source.widgets.shift();
        }
      }
    }

    function fillStructure(model, columns, counter){
      angular.forEach(model.rows, function(row){
        angular.forEach(row.columns, function(column){
          if (!column.widgets){
            column.widgets = [];
          }
          if ( columns[counter] ){
            copyWidgets(columns[counter], column);
            counter++;
          }
        });
      });
      return counter;
    }

    function readColumns(model){
      var columns = [];
      angular.forEach(model.rows, function(row){
        angular.forEach(row.columns, function(col){
          columns.push(col);
        });
      });
      return columns;
    }

    function changeStructure(model, structure){
      var columns = readColumns(model);
      model.rows = angular.copy(structure.rows);
      var counter = 0;
      while ( counter < columns.length ){
        counter = fillStructure(model, columns, counter);
      }
    }

    function createConfiguration(type){
      var cfg = {};
      var config = dashboard.widgets[type].config;
      if (config){
        cfg = angular.copy(config);
      }
      return cfg;
    }

    return {
      replace: true,
      restrict: 'EA',
      transclude : false,
      scope: {
        structure: '@',
        name: '@',
        collapsible: '@',
        adfModel: '=',
        adfWidgetFilter: '='
      },
      controller: function($scope){
        // sortable options for drag and drop
        $scope.sortableOptions = {
          connectWith: ".column",
          handle: ".fa-arrows",
          cursor: 'move',
          tolerance: 'pointer',
          placeholder: 'placeholder',
          forcePlaceholderSize: true,
          opacity: 0.4
        };


        var model = {};
        var structure = {};
        var widgetFilter = {};
        var structureName = {};
        var name = $scope.name;

        // Watching for changes on adfModel
        $scope.$watch('adfModel', function(oldVal, newVal) {
          if(newVal =! null) {
            model = $scope.adfModel;
            widgetFilter = $scope.adfWidgetFilter;
            if ( ! model || ! model.rows ){
              structureName = $scope.structure;
              structure = dashboard.structures[structureName];
              if (structure){
                if (model){
                  model.rows = angular.copy(structure).rows;
                } else {
                  model = angular.copy(structure);
                }
                model.structure = structureName;
              } else {
                $log.error( 'could not find structure ' + structureName);
              }
            }

            if (model) {
              if (!model.title){
                model.title = 'Dashboard';
              }
              $scope.model = model;
            } else {
              $log.error('could not find or create model');
            }
          }
        }, true);

        // edit mode
        $scope.editMode = false;
        $scope.editClass = "";

        $scope.toggleEditMode = function(){
          $scope.editMode = ! $scope.editMode;
    		  if ($scope.editMode){
            $scope.modelCopy = angular.copy($scope.adfModel, {});
    		  }

          if (!$scope.editMode){
            $rootScope.$broadcast('adfDashboardChanged', name, model);
          }
        };

        $scope.cancelEditMode = function(){
          $scope.editMode = false;
		      $scope.modelCopy = angular.copy($scope.modelCopy, $scope.adfModel);
        };

        // edit dashboard settings
        $scope.editDashboardDialog = function(){
          var editDashboardScope = $scope.$new();
          editDashboardScope.structures = dashboard.structures;
          var instance = $modal.open({
            scope: editDashboardScope,
            templateUrl: adfTemplatePath + 'dashboard-edit.html'
          });
          $scope.changeStructure = function(name, structure){
            $log.info('change structure to ' + name);
            changeStructure(model, structure);
          };
          editDashboardScope.closeDialog = function(){
            instance.close();
            editDashboardScope.$destroy();
          };
        };

        // add widget dialog
        $scope.addWidgetDialog = function(){
          var addScope = $scope.$new();
          var widgets;
          if (angular.isFunction(widgetFilter)){
            widgets = {};
            angular.forEach(dashboard.widgets, function(widget, type){
              if (widgetFilter(widget, type)){
                widgets[type] = widget;
              }
            });
          } else {
            widgets = dashboard.widgets;
          }
          addScope.widgets = widgets;
          var opts = {
            scope: addScope,
            templateUrl: adfTemplatePath + 'widget-add.html'
          };
          var instance = $modal.open(opts);
          addScope.addWidget = function(widget){
            var w = {
              type: widget,
              config: createConfiguration(widget)
            };
            addScope.model.rows[0].columns[0].widgets.unshift(w);
            instance.close();

            addScope.$destroy();
          };
          addScope.closeDialog = function(){
            instance.close();
            addScope.$destroy();
          };
        };
      },
      link: function ($scope, $element, $attr) {
        // pass attributes to scope
        $scope.name = $attr.name;
        $scope.structure = $attr.structure;
      },
      templateUrl: adfTemplatePath + 'dashboard.html'
    };
  });
