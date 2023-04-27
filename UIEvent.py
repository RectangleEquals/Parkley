class UIEventCallbacks:
    def __init__(self):
        self.on_cancel_process = None


class UIEvent:
    def __init__(self, event_type, event_data=None):
        self.event_type = event_type
        self.event_message = event_data
