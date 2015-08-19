# author vishwanath subramanian
import boto
import boto.ec2
import boto.rds
import boto.emr
import boto.redshift
import boto.ec2.cloudwatch
import datetime
from datetime import timedelta


emr = boto.connect_emr()
ec2 = boto.connect_ec2()
cw = boto.ec2.cloudwatch.connect_to_region('us-east-1')
all_running_clusters = []
all_ins_names = []
today = datetime.datetime.now() - timedelta(hours=7)

states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING', 'TERMINATED', 'TERMINATED_WITH_ERRORS', 'TERMINATING']

def check_emr():
    global hour_threshold
    hour_threshold = 4
    states = ['RUNNING', 'WAITING']
    global data
    data = []
    for a in emr.list_clusters(cluster_states=states).clusters:

        tdelta = today - (
        datetime.datetime.strptime(a.status.timeline.creationdatetime, "%Y-%m-%dT%H:%M:%S.%fZ") - timedelta(hours=7))

        if tdelta > datetime.timedelta(hours=hour_threshold):
            print "EMR Cluster runnning for over " + str(tdelta) + " hours. "
            runtime = (
            datetime.datetime.strptime(a.status.timeline.creationdatetime, "%Y-%m-%dT%H:%M:%S.%fZ") - timedelta(
                hours=7))
            print "Name: ", a.name, "Status: ", a.status.state, " Created Time : ", runtime, tdelta

            c =  emr.list_instance_groups(a.id).instancegroups
            for z in c:
                print z.name,z.instancetype,z.requestedinstancecount

            print "-------------------------------------------------------------------"
            b = "EMR Cluster runnning for over " + str(tdelta) + " hours,  " + "Name: " + (
            a.name) + "Status: " + a.status.state + " Created Time : " + str(runtime) + " " + str(
                tdelta) + " "
            data.append(b)
            

if __name__ == '__main__':
    check_emr()


