/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')

  .config(['$stateProvider',
    function($stateProvider) {
      'use strict';

      $stateProvider
        .state('compose_app', {
          url: '/apps/compose',
          templateUrl: 'views/apps/compose/compose.html',
          controller: 'ComposeAppCtrl',
          resolve: {
            partitioners: ['models', function(models) {
              return models.$get.partitioners();
            }]
          }
        });
    }])

  // todo: remove the leading $ and rename $visNetworkStyle to vis, internal module does not have a $.
  .controller('ComposeAppCtrl', ['$scope', '$state', '$modal', '$contextmenu',
    'models', 'partitioners', '$visNetworkStyle', 'composeAppDialogs',
    function($scope, $state, $modal, $contextmenu, models, partitioners, $vis, dialogs) {
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

      $scope.chooseProcessor = function(processor) {
        var args = {
          processor: processor
        };
        chooseProcessorDialog.show(args, function(processor) {
          processor.processorId = processor.processorId || newProcessorId();
          $scope.visGraph.data.nodes.update(angular.merge({
            id: processor.processorId,
            label: processor.taskClass
          }, processor));
        });
      };

      $scope.chooseEdge = function(edge) {
        var args = {
          edge: edge,
          partitioners: partitioners,
          processors: {}
        };
        angular.forEach($scope.visGraph.data.nodes, function(processor) {
          args.processors[processor.processorId] = {
            text: 'Processor ' + processor.processorId,
            subtext: processor.taskClass
          };
        });
        chooseEdgeDialog.show(args, function(edge) {
          $scope.visGraph.data.edges.update(angular.merge({
            id: edge.edgeId
          }, edge));
        });
      };

      var processorId = 0;

      function newProcessorId() {
        return ++processorId;
      }

      var visGraphOptions = $vis.newOptions({depth: 1});
      delete visGraphOptions.layout;
      $scope.visGraph = {
        options: visGraphOptions,
        data: $vis.newData(),
        events: {
          doubleClick: function(data) {
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
          oncontext: function(data) {
            var elem = document.getElementById('contextmenu');
            if (data.hasOwnProperty('node')) {
              $scope.selectItemModify = function() {
                $scope.chooseProcessor($scope.visGraph.data.nodes.get(data.node));
              };
              $scope.selectItemDelete = function() {
                $scope.visGraph.data.nodes.remove(data.node);
              };
              $contextmenu.popup(elem, data.pointer.DOM);
            } else if (data.hasOwnProperty('edge')) {
              $scope.selectItemModify = function() {
                $scope.chooseEdge($scope.visGraph.data.edges.get(data.edge));
              };
              $scope.selectItemDelete = function() {
                $scope.visGraph.data.edges.remove(data.edge);
              };
              $contextmenu.popup(elem, data.pointer.DOM);
            }
          }
        }
      };

      $scope.files = {};
      $scope.submitted = false;

      $scope.$watch('uploads', function(uploads) {
        $scope.files = {}; // todo: only one file can be uploaded once (issue 1450)
        angular.forEach(uploads, function(file) {
          if (_.endsWith(file.name, '.jar')) {
            $scope.files[file.name] = file;
          }
        });
      });

      $scope.removeFile = function(name) {
        delete $scope.files[name];
      };

      $scope.submit = function() {
        var data = $scope.visGraph.data;
        var processors = data.nodes.get();
        var edges = data.edges.get().filter(function(edge) {
          return data.nodes.get(edge.from) && data.nodes.get(edge.to);
        });
        var args = {
          appName: 'userapp',
          processors: processors.map(function(processor) {
            return [processor.processorId, {
              id: processor.processorId,
              taskClass: processor.taskClass,
              parallelism: processor.parallelism,
              description: processor.description
            }];
          }),
          dag: {
            vertexList: _.pluck(processors, 'processorId'),
            edgeList: edges.map(function(edge) {
              return [edge.from, edge.partitionerClass, edge.to]
            })
          }
        };

        $scope.submitting = true;
        models.submitDag($scope.files, args, function(response) {
          $scope.submitting = false;
          $scope.submitted = response.success;
          $scope.shouldNoticeSubmitFailed = !response.success;
          if (response.success) {
            $scope.appId = response.appId;
          } else {
            $scope.error = response.error;
            $scope.hasStackTrace = response.stackTrace.length > 0;
            $scope.showErrorInNewWin = function() {
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

      $scope.view = function() {
        $state.go('streamingapp.overview', {appId: $scope.appId});
      }

      // Angular template cannot call the function directly, so export a function.
      $scope.keys = Object.keys;
    }])

  .factory('composeAppDialogs', ['$modal', function($modal) {
    'use strict';

    return {
      create: function(options) {
        var dialog = $modal({
          scope: options.scope,
          templateUrl: options.templateUrl,
          controller: options.controller,
          backdrop: 'static',
          keyboard: true,
          show: false
        });

        var showDialogFn = dialog.show;
        dialog.show = function(args, onChange) {
          angular.merge(dialog.$options.scope, args);
          dialog.$options.scope.onChange = onChange;
          showDialogFn();
        };

        return dialog;
      }
    };
  }])
;