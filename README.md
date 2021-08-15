# ``mas_execution_manager``

# Table of Contents
- [``mas_execution_manager``](#mas_execution_manager)
- [Table of Contents](#table-of-contents)
  - [Summary](#summary)
    - [Action execution](#action-execution)
    - [Scenario execution](#scenario-execution)
  - [Package organisation](#package-organisation)
  - [Dependencies](#dependencies)
  - [Scenario execution](#scenario-execution-1)
    - [State machine configuration file syntax](#state-machine-configuration-file-syntax)
    - [State implementations](#state-implementations)
    - [Examples](#examples)
    - [Launch file parameters](#launch-file-parameters)
    - [MAS domestic-specific scenario state base](#mas-domestic-specific-scenario-state-base)
  - [Component State Machine](#component-state-machine)

## Summary

The `mas_execution_manager` includes various Python-based components that aim to simplify the process of working with finite state machines for developing robot applications. The execution manager is particularly focused on two aspects:
* fault-tolerant action execution
* scenario definition and execution

The components in this repository are primarily used in the [mas_domestic_robotics](https://github.com/b-it-bots/mas_domestic_robotics) repository, but are kept separate in the hope that they will also be useful elsewhere.

### Action execution

To ensure a unified action execution development framework and encode explicit recovery procedures in the execution process, we model robot actions using fault-tolerant state machines (FTSMs); a definition and an implementation of a fault-tolerant state machine can be found [here](https://github.com/b-it-bots/ftsm). A fault-tolerant state machine includes five different states in which an action can be - `init`, `configuring`, `ready`, `running`, and `recovering` - and has a well-defined set of allowed transitions between the states.

In this repository, we define a base class for implementing fault-tolerant actions that inherits from the [FTSM base class](https://github.com/b-it-bots/ftsm/blob/master/pyftsm/ftsm.py) and provides an interface to the the action execution ontology defined at [https://github.com/b-it-bots/action-execution](https://github.com/b-it-bots/action-execution).

### Scenario execution

In order to simplify the process of defining state machines for complex robot scenarios, we define a framework for creating and executing a [smach](http://wiki.ros.org/smach) state machine from a state machine description specified in a configuration file.

State machine configuration files can be specified in two different ways:

* independently, i.e. a single configuration file fully describes a state machine
* one state machine can inherit the structure from another state machine, but can also add new states and remove/replace states that exist in the parent state machine

The latter allows defining generic/robot-independent state machines that can then be made specific by a robot-dependent definition. For example, the generic definition might contain an `arm_moving` state that might be defined somewhere in `mas_domestic_robotics`; however, this state might need to be reimplemented for a particular robot, such that the child configuration file allows us to specify the new state without redefining the complete state machine.

State machines can also be resumed if states are saved as `mdr_monitoring_msgs/ExecutionState` messages using `mongodb_store`. Saving the state and resuming the execution can be enabled/disabled by passing the parameters `save_sm_state` and `recover_sm` when starting the creator node, which is launched by the `state_machine_creator` executable.

The package also defines a scenario state base class that defines various functionalities for interacting with rosplan and is thus suitable for prototyping plan-based scenario implementations without actually planning (an example of such a scenario implementation can be found [here](https://github.com/b-it-bots/mas_domestic_robotics/tree/kinetic/mdr_planning/mdr_scenarios/mdr_robocup_tasks/mdr_store_groceries)).

## Package organisation

The package has the following structure:
```
mas_execution_manager
|    requirements.txt
|    package.xml
|    CMakeLists.txt
|    setup.py
|    README.md
|____common
|    |____mas_execution
|         |    __init__.py
|         |    action_sm_base.py
|         |    sm_params.py
|         |____sm_loader.py
|____ros
|    |____src
|    |    |____mas_execution_manager
|    |    |    |    __init__.py
|    |    |    |____scenario_state_base.py
|    |    |
|    |____scripts
|         |____state_machine_creator
|____component_sm
     |    component_sm_base.py
     |    rgbd_camera_sm.py
     |____run_rgbd_camera.py
```

## Dependencies

* ``importlib``
* ``rospy``
* ``smach``
* ``smach_ros``
* [``mas_knowledge_base``](https://github.com/b-it-bots/mas_knowledge_base)
* [``ftsm``](https://github.com/b-it-bots/ftsm)
* ``mdr_monitoring_msgs``

## Scenario execution

### State machine configuration file syntax

A state machine definition file is a `yaml` file with the following structure:
```
sm_id: <string> | required
states: list<string> | required
outcomes: list<string> | required
state_descriptions: | required
    - state:
        name: <string> | required
        state_module_name: <string> | required
        state_class_name: <string> | required
        transitions: | required
            - transition:
                name: <string> | required
                state: <string> | required
            - transition:
                ...
        arguments: | optional
            - argument:
                name: <string> | required
                value: <Python type> | required
            - argument:
                ...
    - state:
        ...
```

The configuration file keys have the following semantics:

* `sm_id`: a unique ID for the state machine
* `states`: a list of names for the state machine's states
* `outcomes`: a list of names of the state machine's terminal outcomes
* For each state, we have:
    * `name`: the state's name
    * `state_module_name`: the name of a module in which the state is defined
    * `state_class_name`: the name of the class that defines the state
    * For each transition, we have:
        * `name`: the transition's name
        * `state`: the name of the resulting state after the transition
    * If any parameters need to be passed to the state when it is being created, we can specify them here. For each of these, we have:
        * `name`: the argument's name
        * `value`: the argument's value

If a configuration file is supposed to inherit the structure from a parent configuration file, the configuration file has the following syntax:
```
sm_id: <string> | optional
states: list<string> | optional
outcomes: list<string> | optional
state_descriptions: | required
    - state:
        name: <string> | required
        remove: True
    - state:
        name: <string> | required
        state_module_name: <string> | required
        state_class_name: <string> | required
        transitions: | required
            - transition:
                name: <string> | required
                state: <string> | required
            - transition:
                ...
        arguments: | optional
            - argument:
                name: <string> | required
                value: <Python type> | required
            - argument:
                ...
```

The following rules apply for the child state machine configuration:

* `sm_id`: if not specified, the ID of the parent state machine will be used
* `states`: if not specified, the list of states specified in the parent's state machine will be used. States that are specified here, but are not in the parent's state machine will be added to the list of states
* `outcomes`: if not specified, the list of outcomes specified in the parent's state machine will be used. Outcomes that are specified here, but are not in the parent's state machine will be added to the list of outcomes
* For each state, if the `remove` key is defined, the state will be removed from the list of states (provided that it is already there). On the other hand, if a state has the same `name` as a state in the parent state machine configuration, but is completely redefined in the child configuration, the definition of the state from the child configuration file will be used

Our state machine specification has various similarities with [AADL](http://www.aadl.info/aadl/currentsite/) (compare for example https://github.com/osate/examples/blob/master/robot/emv2-behavior/robot.aadl); however, given that our focus here is only on state machines, the syntax is more lightweight. In addition, as discussed before, we are interested in state machine inheritance, which is a core aspect of our specification.

### State implementations

Loading a state machine through a configuration file imposes some constraints on the implementation of a state. In particular, a state cannot have named arguments unless they have default values; in addition, all arguments that are defined in the configuration file are passed to the state's constructor as `kwargs`.

A prototypical definition of a smach state that can be used together with a state machine definition file is shown below:
```
class StateName(smach.State):
    def __init__(self, **kwargs):
        smach.State.__init__(self, outcomes=['outcome_1', ... 'outcome_n'],
                             output_keys=['key_1, ..., key_n'])
        self.sm_id = kwargs.get('sm_id', '')
        self.state_name = kwargs.get('state_name', '')
        self.arg = kwargs.get('arg_name', <default_value>)
        ...

    def execute(self, userdata):
        ...
```


### Examples

The example below shows a state machine definition for a simple scenario in which a robot needs to pick a bottle from a table.

```
sm_id: pick_bottle_from_table
states: [GO_TO_TABLE, FIND_OBJECT, GRASP_OBJECT]
outcomes: [DONE, FAILED]
state_descriptions:
    - state:
        name: GO_TO_TABLE
        state_module_name: mdr_navigation_states.states
        state_class_name: MoveBase
        transitions:
            - transition:
                name: succeeded
                state: FIND_OBJECT
            - transition:
                name: failed
                state: GO_TO_TABLE
            - transition:
                name: failed_after_retrying
                state: FAILED
        arguments:
            - argument:
                name: destination_locations
                value: [TABLE]
            - argument:
                name: number_of_retries
                value: 3
    - state:
        name: FIND_OBJECT
        state_module_name: mdr_perception_states.states
        state_class_name: LocateObject
        transitions:
            - transition:
                name: succeeded
                state: GRASP_OBJECT
            - transition:
                name: failed
                state: FIND_OBJECT
            - transition:
                name: failed_after_retrying
                state: FAILED
        arguments:
            - argument:
                name: object
                value: bottle_n
            - argument:
                name: number_of_retries
                value: 3
    - state:
        name: GRASP_OBJECT
        state_module_name: mdr_manipulation_states.states
        state_class_name: Pick
        transitions:
            - transition:
                name: succeeded
                state: DONE
            - transition:
                name: failed
                state: GRASP_OBJECT
            - transition:
                name: failed_after_retrying
                state: FAILED
        arguments:
            - argument:
                name: object
                value: bottle_n
            - argument:
                name: number_of_retries
                value: 3
```

This configuration file defines the following state machine:

![Pick bottle from table state machine](docs/figures/pick_bottle_from_table_sm.png)


The three states of the state machine will be implemented as smach states as shown below:
```
class MoveBase(smach.State):
    def __init__(self, **kwargs):
        smach.State.__init__(self, outcomes=['succeeded', 'failed', 'failed_after_retrying'])
        self.sm_id = kwargs.get('sm_id', '')
        self.state_name = kwargs.get('state_name', '')
        self.destination_locations = kwargs.get('destination_locations', list())
        self.number_of_retries = kwargs.get('number_of_retries', 0)
        self.retry_count = 0
        ...

    def execute(self, userdata):
        ...
        success = <wait for the base to move and get the result>
        if success:
            return 'succeeded'
        else:
            if self.retry_count == self.number_of_retries:
                return 'failed_after_retrying'
            else:
                self.retry_count += 1
                return 'failed'


class LocateObject(smach.State):
    def __init__(self, **kwargs):
        smach.State.__init__(self, outcomes=['succeeded', 'failed', 'failed_after_retrying'],
                             output_keys=['object_pose'])
        self.sm_id = kwargs.get('sm_id', '')
        self.state_name = kwargs.get('state_name', '')
        self.object = kwargs.get('object', '')
        self.number_of_retries = kwargs.get('number_of_retries', 0)
        self.retry_count = 0
        ...

    def execute(self, userdata):
        ...
        success = <try to find the object and wait for the result>
        if success:
            return 'succeeded'
        else:
            if self.retry_count == self.number_of_retries:
                return 'failed_after_retrying'
            else:
                self.retry_count += 1
                return 'failed'


class Pick(smach.State):
    def __init__(self, **kwargs):
        smach.State.__init__(self, outcomes=['succeeded', 'failed', 'failed_after_retrying'])
        self.sm_id = kwargs.get('sm_id', '')
        self.state_name = kwargs.get('state_name', '')
        self.object = kwargs.get('object', '')
        self.number_of_retries = kwargs.get('number_of_retries', 0)
        self.retry_count = 0
        ...

    def execute(self, userdata):
        ...
        success = <try to grasp the object and wait for the result>
        if success:
            return 'succeeded'
        else:
            if self.retry_count == self.number_of_retries:
                return 'failed_after_retrying'
            else:
                self.retry_count += 1
                return 'failed'
```

### Launch file parameters

The following parameters may be passed when launching the execution manager:
* ``sm_config_file``: An absolute path of a state machine definition file
* ``parent_sm_config_file``: An absolute path of a parent state machine definition file (optional; if there is no parent definition file, it is best to delete this parameter in order to avoid undesired parent-children state machine relationships)
* ``save_sm_state``: Specifies whether the current state of the state machine should be saved for the purpose of later recovery (default False)
* ``recover_sm``: Specifies whether a state machine should continue the execution from the last saved state (default False)

### MAS domestic-specific scenario state base

The state machine creator uses Smach, so it is sufficient if the states specified in the definition file inherit from `smach.State` as shown above.

If the skill-based architecture in [`mas_domestic_robotics`](https://github.com/b-it-bots/mas_domestic_robotics) is additionally used, the `ScenarioStateBase` class defined in `scenario_state_base.py` (which inherits from `smach.State`) provides some additional functionalities for more structured action execution (in particular, knowledge base management with the help of [ROSPlan](https://github.com/b-it-bots/ROSPlan) as well as action dispatching to the action clients in `mas_domestic_robotics`). To manage knowledge and enable the invocation of the action clients, the `ScenarioStateBase` class defines the following members:
* `sm_id`: ID of the state machine to which the state belongs
* `state_name`: Name of the state
* `action_name`: Name of an action that should be invoked from the state (as specified in the action client launcher, e.g. the [pickup client](https://github.com/b-it-bots/mas_domestic_robotics/blob/kinetic/mdr_planning/mdr_actions/mdr_manipulation_actions/mdr_pickup_action/ros/launch/pickup_client.launch))
* `retry_count`: Number of times some an execution has been retried (used if the state implements recovery behaviours)
* `executing`: Indicates whether an action has been dispatched and is being executed
* `succeeded`: Indicates whether the execution of an action has succeeded
* `action_dispatch_pub`: The action clients in `mas_domestic_robotics` wait for requests by subscribing to the `/kcl_rosplan/action_dispatch` topic (of type `rosplan_dispatch_msgs.ActionDispatch`); this is a publisher for this topic
* `robot_name`: The name of the robot (optional to specify, but useful in a multi-robot scenario)
* `kb_interface`: An instance of `mas_execution_manager.domestic_kb_interface.DomesticKbInterface` providing functionalities for interacting with a knowledge base

The following member functions are also defined in the base class:
* `execute`: Method in which the execution of the state is taking place. Needs to be overriden by child classes
* `get_dispatch_msg`: Creates a `rosplan_dispatch_msgs.ActionDispatch` message for the action client associated with the state. Needs to be overriden by child classes
* `save_current_state`: Saves the current state in a `mongodb_store` database
* `get_action_feedback`: The action clients in `mas_domestic_robotics` publish feedback to the `/kcl_rosplan/action_feedback` topic; the scenario state base has a subscriber for that topic - this method is the subscriber callback, setting the values of `executing` and `succeeded` depending on the message content
* `say`: The scenario state base also has a publisher for the `/say` topic; this method takes a sentence as a string and publishes that one to `/say` so that a robot can vocalise it

A prototypical definition of a state for using the MAS-specific scenario state base is shown below:
```
class StateName(ScenarioStateBase):
    def __init__(self, save_sm_state=False, **kwargs):
        super(ScenarioStateBase, self).__init__('action_name',
                                                save_sm_state=save_sm_state,
                                                outcomes=['outcome_1', ... 'outcome_n'],
                                                output_keys=['key_1, ..., key_n'])
        self.sm_id = kwargs.get('sm_id', '')
        self.state_name = kwargs.get('state_name', '')
        self.arg = kwargs.get('arg_name', <default_value>)
        ...

    def execute(self, userdata):
        ...
```

If the states in a scenario state machine inherit from `ScenarioStateBase`, it is necessary to launch the ROSPlan components and mongodb_store together with the state machine creator. The [`mdr_rosplan_interface`](https://github.com/b-it-bots/mas_domestic_robotics/tree/devel/mdr_planning/mdr_rosplan_interface) package includes a launcher for these components. If states inherit from `ScenarioStateBase`, but do not use any of the functionalities of ROSPlan or mongodb_store, simply including this launcher is sufficient (as shown [here](https://github.com/b-it-bots/mas_domestic_robotics/blob/devel/mdr_planning/mdr_scenarios/mdr_demos/mdr_demo_describe_people/ros/launch/describe_people.launch)); if these functionalities are used, some of the arguments in the launch file need to be set (as illustrated [here](https://github.com/b-it-bots/mas_domestic_robotics/blob/devel/mdr_planning/mdr_scenarios/mdr_demos/mdr_demo_simple_pick_and_place/ros/launch/pick_and_place.launch)).

## Component State Machine
The Component State Machine is the additional project that contains concept for implementation of the Fault Tolerant Robot Components. The code as well as more detailed explanation are contained in the directory [component_sm](component_sm).