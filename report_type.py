#!/usr/bin/env python3
class Report_Type:
    EMPLOYEE_LIST = 'employee_list'
    LEAVE_TAKEN = 'leave_taken'
    LEAVE_ENT = 'leave_ent'
    PERFORMANCE = 'performance'
    DEMOGRAPHICS = 'demographics'

    def __init__(self, name, key_cols):
        self.name = name
        self.key_cols = key_cols
