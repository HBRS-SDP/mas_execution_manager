from pyftsm.ftsm import FTSMTransitions
from pyftsm.ftsm import FTSM, FTSMTransitions

class ComponentSM(FTSM):
    def __init__(self, name, dependencies, max_recovery_attempts=1):
        super(ComponentSM, self).__init__(name, 
                                          dependencies, 
                                          max_recovery_attempts)
        self.execution_requested = False
        self.result = None

    def ready(self):
        if self.execution_requested:
            self.result = None
            self.execution_requested = False
            return FTSMTransitions.RUN
        else:
            if self.result:
                self.result = None
            return FTSMTransitions.WAIT

    def running(self):
        return FTSMTransitions.DONE

    def recovering(self):
        return FTSMTransitions.DONE_RECOVERING