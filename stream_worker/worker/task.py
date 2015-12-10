# -*- coding: utf-8 -*-
#
#  Copyright 2015 AirPlug Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


import uuid
try:
    import scubagear.logger as log
except ImportError:
    import logging as log


class TaskManager(object):
    IDX_WORKER = 0
    IDX_TOTAL = 1

    def __init__(self, metaHandler):
        """
        TaskDistributor

        :param metaHandler: meta handler instance
        :return:
        """
        self.metaClient = metaHandler

    def __initTaskMapTable(self, excludeTasks=[]):
        """
        Create current allocated task map. exclude 'excludeTasks' and 'consistent-hashing' task
        :param excludeTasks: exclude task list
        :return: None
        """

        # only for production worker
        workers = dict((name, info) for name, info in self.metaClient.getNodes().iteritems() if info['environment'] == 'production')
        tasks = self.metaClient.getTasks()

        # get task list of worker
        if 'all' in excludeTasks:
            # Reset all task except consistent-hashing
            funcFilter = lambda x: tasks[x]['useHashing']
        else:
            # Reset given or consistent-hashing task
            funcFilter = lambda x: x not in excludeTasks or tasks[x]['useHashing']

        self.mapWorkerTask = dict([(name, filter(funcFilter, info['tasks'])) for name, info in workers.iteritems()])

        log.debug('CurrentTaskMap - %s' % self.mapWorkerTask)

    def __assignTask(self, task, count=1):
        """
        Get allocated worker list of given task (with count)

        :param task: task name
        :return: worker list and alloc count (dict)
        """

        #workers = []

        for i in range(count):
            w, s = min([(worker, len(tasks) * (tasks.count(task) + 1)) for worker, tasks in self.mapWorkerTask.iteritems()], key=lambda x: x[1])
            self.mapWorkerTask[w].append(task)

        #print "MAP!@", self.mapWorkerTask

    def rebalance(self, tasks=None):
        """

        :param task: task list to re-balancing ('all' for re-balance all tasks)
        :return: None
        """
        if not tasks or len(tasks) == 0:
            log.warn("invalid task list for rebalance - %s" % tasks)
            return

        self.__initTaskMapTable(excludeTasks=tasks)
        rebalanceTasks = []

        try:
            taskInfos = self.metaClient.getTasks()
            workers = self.metaClient.getNodes()
        except Exception, e:
            print e

        if 'all' in tasks:
            # get current task status
            totalTaskSlot = sum(t['maxWorker'] for t in taskInfos.values() if t['module'] is not None)
            totalWorkerSlot = sum(w['maxSlot'] for w in workers.values() if w['environment'] == 'production')
            tasks = filter(lambda x: not taskInfos[x]['useHashing'] and taskInfos[x]['module'] is not None, taskInfos.keys())
        else:
            (maxSlots, workerTasks) = zip(*[(w['maxSlot'], w['tasks']) for w in workers.values()])

            totalTaskSlot = sum(taskInfos[t]['maxWorker'] for t in tasks)
            totalWorkerSlot = sum(maxSlots) - len(workerTasks) + sum(workerTasks.count(t) for t in tasks)

            if totalTaskSlot > totalWorkerSlot:
                log.warn("needs Scale-out!!!!!")
                # TODO : add fork worker routine.
                return


        log.info("Total Required slot : %d / Total Slot : %d" % (totalTaskSlot, totalWorkerSlot))

        # Get allocated slots per task
        for task in tasks:
            cnt = taskInfos[task]['maxWorker']
            try:
                self.__assignTask(task, cnt)
            except Exception, e:
                log.error(e)

        log.debug("Rebalance result : %s" % self.mapWorkerTask)

        # Update
        evtId = str(uuid.uuid4())
        try:
            for worker, newTasks in self.mapWorkerTask.iteritems():
                event = {}
                event['event'] = 'evtWorkerUpdateTask'
                event['tasks'] = newTasks
                event['evtId'] = evtId
                self.metaClient.sendNodeEvent(worker, event)
        except Exception, e:
            log.error(e)
