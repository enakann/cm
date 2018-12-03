import sys

PROJECT_ROOT = "/home/navi/changemanager/changemanager"
if sys.platform == 'win32':
    PROJECT_ROOT = r"C:\Users\navkanna\PycharmProjects\cm\changemanager"

MSG_TYPE_TABLE_MAPPING = {
    'gen_summary': 'gen_summary',
    'new_policy': 'generate',
    'red_flags': 'generate',
    'existing_policies': 'generate',
    'pre_approved_matched': 'approver',
    'pre_approved_not_matched': 'approver',
    'applier_result': 'applier',
     'change_record':'change_record'}

#from utils.data_store_driver import ConsumerDataStoreDriver,ConsumerDataStoreDriverForGenSummary,ConsumerDataStoreDriverForApprover,ConsumerDataStoreDriverForApplier,ConsDataStoreDrvrForChangeRecordCreator

TABLE_DATA_STORE_DRIVER_CLASS = {
    'gen_summary': 'ConsumerDataStoreDriverForGenSummary',
    'new_policy': 'ConsumerDataStoreDriver',
    'red_flags': 'ConsumerDataStoreDriver',
    'existing_policies': 'ConsumerDataStoreDriver',
    'pre_approved_matched': 'ConsumerDataStoreDriverForApprover',
    'pre_approved_not_matched': 'ConsumerDataStoreDriverForApprover',
    'applier_result': 'ConsumerDataStoreDriverForApplier',
     'change_record':'ConsDataStoreDrvrForChangeRecordCreator'}


MSG_TYPE_CONATINER_MAP = {'gen_summary':'MessageInfos',
                          'new_policy':'GenMessageInfos',
                          'red_flags':'GenMessageInfos',
                          'existing_policies':'GenMessageInfos',
                          'pre_approved_matched':'ApproverInfos',
                          'pre_approved_not_matched':'ApproverInfos',
                          'applier_result':'ApplierInfos',
                          'change_record':''}

