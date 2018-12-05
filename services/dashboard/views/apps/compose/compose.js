/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
angular.module('dashboard')

  .config(['$stateProvider',
    function ($stateProvider) {
      'use strict';

      $stateProvider
        .state('compose_app', {
          url: '/apps/compose',
          templateUrl: 'views/apps/compose/compose.html',
          controller: 'ComposeAppCtrl',
          resolve: {
            partitioners: ['models', function (models) {
              return models.$get.partitioners();
            }]
          }
        });
    }])

  // todo: remove the leading $ and rename $visNetworkStyle to vis, internal module does not have a $.
  .controller('ComposeAppCtrl', ['$scope', '$state', '$modal', '$contextmenu',
    'models', 'partitioners', '$visNetworkStyle', 'composeAppDialogs',
    function ($scope, $state, $modal, $contextmenu, models, partitioners, $vis, dialogs) {
      'use strict';

      var chooseProcessorDialog = dialogs.create({
        scope: $scope.$new(true), // isolated child scope
        templateUrl: 'views/apps/compose/popups/choose_processor.html',
        controller: 'ComposeAppChooseProcessorCtrl'
      });

      var chooseEdgeDialog = dialogs.create({
        scope: $scope.$new(true), // isolated child scope
        templateUrl: 'views/apps/compose/popups/choose_edge.html',
        controller: 'ComposeAppChooseEdgeCtrl'
      });

      $scope.appName = "";

      $scope.chooseProcessor = function (processor) {
        var args = {
          processor: processor
        };
        chooseProcessorDialog.show(args, function (processor) {
          if (!processor.hasOwnProperty('id')) {
            processor.id = newProcessorId();
          }
          processor.label = $vis.processorNameAsLabel(processor);
          $scope.visGraph.data.nodes.update(processor);
        });
      };

      $scope.chooseEdge = function (edge) {
        var args = {
          edge: edge,
          partitioners: partitioners,
          processors: {}
        };
        angular.forEach($scope.visGraph.data.nodes, function (processor) {
          args.processors[processor.id] = {
            text: 'Processor ' + processor.id,
            subtext: processor.taskClass
          };
        });
        chooseEdgeDialog.show(args, function (edge) {
          $scope.visGraph.data.edges.update(edge);
        });
      };

      var processorId = 0;

      function newProcessorId() {
        return processorId++;
      }

      $scope.visGraph = {
        options: $vis.newOptions(/*height=*/'400px'),
        data: $vis.newData(),
        events: {
          onDoubleClick: function (data) {
            if (data.nodes.length === 1) {
              var processor = $scope.visGraph.data.nodes.get(data.nodes[0]);
              $scope.chooseProcessor(processor);
            } else if (data.edges.length === 1) {
              var edge = $scope.visGraph.data.edges.get(data.edges[0]);
              $scope.chooseEdge(edge);
            } else if (data.nodes.length + data.edges.length === 0) {
              $scope.chooseProcessor();
            }
          },
          onContext: function (data) {
            if (data.hasOwnProperty('node')) {
              $scope.selectItemModify = function () {
                $scope.chooseProcessor($scope.visGraph.data.nodes.get(data.node));
              };
              $scope.selectItemDelete = function () {
                deleteProcessor(data.node);
              };
            } else if (data.hasOwnProperty('edge')) {
              $scope.selectItemModify = function () {
                $scope.chooseEdge($scope.visGraph.data.edges.get(data.edge));
              };
              $scope.selectItemDelete = function () {
                deleteEdge(data.edge);
              };
            } else {
              return;
            }
            var elem = document.getElementById('contextmenu');
            $contextmenu.popup(elem, data.pointer.DOM);
          },
          onDeletePressed: function (selection) {
            if (selection.nodes.length === 1) {
              deleteProcessor(selection.nodes[0]);
            } else if (selection.edges.length === 1) {
              deleteEdge(selection.edges[0]);
            }
          }
        }
      };

      function deleteProcessor(processorId) {
        $scope.visGraph.data.nodes.remove(processorId);
        var edgeIds = _.chain($scope.visGraph.data.edges.get())
          .filter(function (edge) {
            return processorId == edge.from || processorId == edge.to;
          })
          .map('id')
          .value();
        $scope.visGraph.data.edges.remove(edgeIds);
      }

      function deleteEdge(edgeId) {
        $scope.visGraph.data.edges.remove(edgeId);
      }

      $scope.files = {};
      $scope.submitted = false;

      $scope.$watch('uploads', function (uploads) {
        $scope.files = {}; // todo: only one file can be uploaded once (issue 1450)
        angular.forEach(uploads, function (file) {
          if (_.endsWith(file.name, '.jar')) {
            $scope.files[file.name] = file;
          }
        });
      });

      $scope.removeFile = function (name) {
        delete $scope.files[name];
      };

      $scope.canSubmit = function () {
        return !$scope.submitted &&
          $scope.appName.length > 0 &&
          $scope.visGraph.data.nodes.length > 0 &&
          Object.keys($scope.files).length > 0;
      };

      $scope.submit = function () {
        var data = $scope.visGraph.data;
        var processors = data.nodes.get();
        var edges = data.edges.get();
        var args = {
          appName: $scope.appName,
          processors: processors.map(function (processor) {
            return [processor.id, {
              id: processor.id,
              taskClass: processor.taskClass,
              parallelism: processor.parallelism,
              description: processor.description
            }];
          }),
          dag: {
            vertexList: _.map(processors, 'id'),
            edgeList: edges.map(function (edge) {
              return [edge.from, edge.partitionerClass, edge.to]
            })
          },
          userconfig: null
        };

        $scope.submitting = true;
        models.submitDag($scope.files, args, function (response) {
          $scope.submitting = false;
          $scope.submitted = response.success;
          $scope.shouldNoticeSubmitFailed = !response.success;
          if (response.success) {
            $scope.appId = response.appId;
          } else {
            $scope.error = response.error;
            $scope.hasStackTrace = response.stackTrace.length > 0;
            $scope.showErrorInNewWin = function () {
              if ($scope.hasStackTrace) {
                var popup = window.open('', 'Error Log');
                var html = [$scope.error].concat(response.stackTrace).join('\n');
                popup.document.open();
                popup.document.write('<pre>' + html + '</pre>');
                popup.document.close();
              }
            }
          }
        });
      };

      $scope.view = function () {
        $state.go('streamingapp.overview', {appId: $scope.appId});
      };

      // Angular template cannot call the function directly, so export a function.
      $scope.keys = Object.keys;
    }])

  .factory('composeAppDialogs', ['$modal', function ($modal) {
    'use strict';

    return {
      create: function (options) {
        var dialog = $modal({
          scope: options.scope,
          templateUrl: options.templateUrl,
          controller: options.controller,
          backdrop: 'static',
          keyboard: true,
          show: false
        });

        var showDialogFn = dialog.show;
        dialog.show = function (args, onChange) {
          dialog.$options.scope.onChange = onChange;
          angular.forEach(args, function (value, key) {
            dialog.$options.scope[key] = value;
          });
          showDialogFn();
        };

        return dialog;
      }
    };
  }])
;
