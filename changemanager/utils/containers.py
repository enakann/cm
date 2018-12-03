import logging
import sys
PROJECT_ROOT = "/home/navi/Desktop/changemanager"
if sys.platform == 'win32':
    PROJECT_ROOT = r"C:\Users\navkanna\PycharmProjects\cm\changemanager"
    
logger=logging.getLogger("kannan")

class Summary:
    def __init__(self, msg):
        self.msg = msg
        self.correlation_id = self.msg[2]
        self.username = self.msg[3]
        self.ticket_no = self.msg[4]
        self.type = self.msg[5]
        self.total_recs = self.msg[6]
        self.recomm_for = self.msg[7]
        self.existing = self.msg[8]
        self.red_flags = self.msg[9]
        self.payload = self.msg[10]
        self.status = self.msg[11]
        self.failed=[]
        self.table='gen_summary'
    
    def __repr__(self):
        return "Summary(correlation_id={},total_recs={}, recomm_for={}, existing={}, red_flags={})". \
            format (self.correlation_id, self.total_recs, self.recomm_for, self.existing, self.red_flags)
    
    def as_list(self):
        return self.msg
    
    def get_header(self):
        _header = dict ()
        _header["correlation_id"] = self.correlation_id
        _header["username"] = self.username
        _header["ticket_num"] = self.ticket_no
        _header["type"] = "change_record"
        return _header
    
    def get_payload(self):
        return self.payload


class GenMessages (object):
    def __init__(self, msg):
        #        print("in gen messagess {}".format(msg))
        self.msg = msg
        self.correlation_id = self.msg[2]
        self.username = self.msg[3]
        self.ticket_no = self.msg[4]
        self.type = self.msg[5]
        self.payload = self.msg[6]
        self.status = self.msg[7]
        self.table='generator'
    
    def __repr__(self):
        return "{}(correlation_id ={},type={}, status={})". \
            format (self.__class__.__name__, self.correlation_id, self.type, self.status)
    
    def as_list(self):
        return self.msg
    
    def get_header(self):
        _header = dict ()
        _header["correlation_id"] = self.correlation_id
        _header["username"] = self.username
        _header["ticket_num"] = self.ticket_no
        _header["type"] = "change_record"
        return _header
    
    def get_payload(self):
        return self.payload
    
    # -----------------------------------------------
    # python2.7
    ##########
    
    """class ApproverApplierMsg(GenMessages):
    def __init__(self,msg):
       super(GenMessages,self).__init__(msg)
       self.count=self.msg[6]
       self.payload = self.msg[7]
       self.status = self.msg[8]"""
    
    # python3
    ########


class ApproverMsg (GenMessages):
    def __init__(self, msg):
        super ().__init__ (msg)
        self.count = self.msg[6]
        self.payload = self.msg[7]
        self.status = self.msg[8]
        self.table='approver'

class ApplierMsg (GenMessages):
    def __init__(self, msg):
        super ().__init__ (msg)
        self.total_failed=self.msg[6]
        self.total_success=self.msg[7]
        self.payload = self.msg[8]
        self.status = self.msg[9]
        self.table='applier'


class MessageInfos (object):
    def __init__(self):
        self.container = []
    
    def add(self, *args):  # Either this
        self.container.append (Summary (*args))
    
    @classmethod
    def from_list(cls, rows):  # Or this
        self = cls ()
        for row in rows:
            self.container.append (Summary (row))
        return self.container
    
    def __getattr__(self, name):
        return getattr (self.container, name)
    
    def __getitem__(self, item):
        return self.container[item]
    
    def __len__(self):
        return len (self.container)


class GenMessageInfos (MessageInfos):
    @classmethod
    def from_list(cls, rows):  # Or this
        self = cls ()
        for row in rows:
            self.container.append (GenMessages (row))
        return self.container


class ApproverInfos (GenMessageInfos):
    @classmethod
    def from_list(cls, rows):  # Or this
        self = cls ()
        for row in rows:
            self.container.append (ApproverMsg (row))
        return self.container


class ApplierInfos (GenMessageInfos):
    @classmethod
    def from_list(cls, rows):  # Or this
        self = cls ()
        for row in rows:
            self.container.append (ApplierMsg (row))
        return self.container
    
    def add(self, *args):  # Either this
        self.container.append (ApplierMsg(*args))

if __name__ == '__main__':
    applier_result=[(1, 1543526467.781958, '99', 'kannan', 'srno123', 'applier_result', '{"applier_result": {"ch3-fa-c4r512-fw-1": {"meta-data": {"model": "SRX", "vendor": "Cisco"}, "cmds": {"new_app_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match application junos-ssh", "result": "Passed"}], "new_src_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}, {"reason": "ssh error failed to connect", "cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "failed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}, {"reason": "Policy cannot be applied Error", "cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "failed"}], "new_dst_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match destination-address b2b22.bankofamerica.com-171.162.110.17", "result": "Passed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match destination-address b2b22.bankofamerica.com-171.162.110.17", "result": "Passed"}]}}, "ch3-fa-c4r512-fw-3": {"meta-data": {"model": "SRX", "vendor": "Cisco"}, "cmds": {"new_app_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match application junos-ssh", "result": "Passed"}], "new_src_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "failed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}], "new_dst_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match destination-address b2b22.bankofamerica.com-171.162.110.17", "result": "Passed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match destination-address b2b22.bankofamerica.com-171.162.110.17", "result": "Passed"}]}}, "ch3-fa-c4r512-fw-2": {"meta-data": {"model": "SRX", "vendor": "Cisco"}, "cmds": {"new_app_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match application junos-ssh", "result": "Passed"}], "new_src_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "failed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match source-address us2-chicago-colo-opc-v142-10.72.21.65/32", "result": "Passed"}], "new_dst_cmd": [{"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match destination-address b2b22.bankofamerica.com-171.162.110.17", "result": "Passed"}, {"cmd": "set security policies from-zone PRIVATE-MT to-zone UNTRUST policy 180814-001010_psane_1 match destination-address b2b22.bankofamerica.com-171.162.110.17", "result": "Passed"}]}}}}', 'pending')]
    #ls=ApplierInfos.from_list(applier_result)
    cont=ApplierInfos()
    cont.add(applier_result[0])
    print(cont.container)
