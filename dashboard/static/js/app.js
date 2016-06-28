"use strict";

var App = angular.module("todo", ["ui.sortable", "LocalStorageModule"]);

App.controller("TodoCtrl", function ($scope, localStorageService) {

	$scope.init = function () {

		if (!localStorageService.get("loList")) {
			$scope.model = [
				{
					name: "Collaboration", list: [
						{ taskName: "Participate in Discussion Forum"}
					]
				},
				{
					name: "Problem Solving", list: [
						{ taskName: "Complete Weekly Problem Sets"},
					]
				}
			];
		}else{
			$scope.model = localStorageService.get("loList");
		}
		$scope.show = "All";
		$scope.currentShow = 0;
	};

	$scope.addTodo = function  () {
		/*Should prepend to array*/
		$scope.model[$scope.currentShow].list.splice(0, 0, {taskName: $scope.newTodo});
		/*Reset the Field*/
		$scope.newTodo = "";
	};

	$scope.deleteTodo = function  (index) {
		$scope.model[$scope.currentShow].list.splice(index, 1);
	};
	
	$scope.addLearningOutcome = function  () {
		/*Should prepend to array*/
		$scope.model.push({name : $scope.newLearningOutcome , list : [] });
		/*Reset the Field*/
		$scope.newLearningOutcome = "";
	};	

	$scope.todoSortable = {
		containment: "parent",//Dont let the user drag outside the parent
		cursor: "move",//Change the cursor icon on drag
		tolerance: "pointer"//Read http://api.jqueryui.com/sortable/#option-tolerance
	};

	$scope.changeTodo = function  (i) {
		$scope.currentShow = i;
	};

	$scope.$watch("model",function  (newVal,oldVal) {
		if (newVal !== null && angular.isDefined(newVal) && newVal!==oldVal) {
			localStorageService.add("loList",angular.toJson(newVal));
		}
	},true);

});