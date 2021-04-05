import subprocess
import shlex
import argparse


def valid_ip(ip):
    return len(ip.split(".")) == 4

def get_ips(file_name, only_data=False):
    """
    assume that the file line will be:
        <id_val>: <ip_addr> or
        <ip_addr> <domain>.local <domain>
    """
    
    global found_ips
    found_ips = 0
    def run_ip(ip, is_data=True):
        valid = valid_ip(ip)
        global found_ips
        if valid: found_ips += 1 # not great
        return valid and (not only_data or is_data)

    ips = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            vals = line.split()
            if len(vals) == 2:
                key = ''.join(filter(str.isalnum, vals[0]))
                addr = vals[1]
                if run_ip(addr, key.isnumeric()): 
                    ips.append(addr)

            elif len(vals) == 3: #alt format from /etc/hosts
                addr = vals[0]
                if run_ip(addr, found_ips>=1): #very first one is namenode
                    ips.append(addr)
    return ips


def run_ips(ips, user, in_cmd, write_out=None):
    ssh_stdout = []
    for ip in ips:
        cmd = shlex.split("ssh {}@{} \"{}\"".format(user, ip, in_cmd))
        ran = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, universal_newlines=True)
        if write_out:
            ssh_stdout.append(ran.stdout)
        else:
            print("finished cmd {} at {} with output:\n{}".format(in_cmd, ip, ran.stdout)) 

    if write_out:
        with open(write_out, "a+") as f:
            f.write('\n'.join(ssh_stdout))

if __name__ == "__main__":
    parse = argparse.ArgumentParser("Runs the command on all ssh ips in the supplied file")
    parse.add_argument("-c", "--cmd",  default='ls', help="command to run in each ip, cannot include double quote (\") characters")
    parse.add_argument("-d", "--data", action="store_true", help="apply to only data ips")
    parse.add_argument("-f", "--file", help="file that contains the ips")
    parse.add_argument("-o", "--out",  help="write output of ssh stdout to file")
    parse.add_argument("-s", "--ssh",  help="single target to run the command on, has precedence over the file arg")
    parse.add_argument("-u", "--user", default='ubuntu', help="the user for the ips, for now all the same")
    args = parse.parse_args()

    if not args.ssh:
        ips = get_ips(args.file, args.data)
    elif valid_ip(args.ssh):
        ips = [args.ssh]
    else:
        ips = []

    if ips != []: run_ips(ips, args.user, args.cmd, args.out)
