# Based on the spark.py created by the hadoop_5k authors
# from https://github.com/mliroz/hadoop_g5k
# Laudin Alessandro Molina <laudin.molina@gmail.com>

import os

from ConfigParser import ConfigParser

from execo.action import TaktukPut, Remote, TaktukRemote, \
    SequentialActions
from execo.log import style
from execo_engine import logger

# Default parameters
DEFAULT_CASSANDRA_BASE_DIR = "/tmp/cassandra"
DEFAULT_CASSANDRA_CONF_DIR = DEFAULT_CASSANDRA_BASE_DIR + "/conf"
DEFAULT_CASSANDRA_LOGS_DIR = DEFAULT_CASSANDRA_BASE_DIR + "/logs"
DEFAULT_CASSANDRA_DATA_DIR = DEFAULT_CASSANDRA_BASE_DIR + "/data"

class CassandraCluster(object):
    """This class manages the whole life-cycle of a Cassandra cluster.

    Attributes:
      seeds (Host):
        The hosts selected as seed
      hosts (list of Hosts):
        List of hosts composing the cluster.
      initialized (bool):
        True if the cluster has been initialized, False otherwise.
      running (bool):
        True if cassandra is running, False otherwise.
    """

    @staticmethod
    def get_cluster_type():
        return "cassandra"

    initialized = False
    running = False

    # Default properties
    defaults = {
        "cassandra_base_dir": DEFAULT_CASSANDRA_BASE_DIR,
        "cassandra_conf_dir": DEFAULT_CASSANDRA_CONF_DIR,
        "cassandra_logs_dir": DEFAULT_CASSANDRA_LOGS_DIR,
        "cassandra_data_dir": DEFAULT_CASSANDRA_DATA_DIR,
    }

    def __init__(self, hosts, config_file=None):

        # Load cluster properties
        config = ConfigParser(self.defaults)
        config.add_section("cluster")

        if config_file:
            config.readfp(open(config_file))

        self.base_dir = config.get("cluster", "cassandra_base_dir")
        self.conf_dir = config.get("cluster", "cassandra_conf_dir")
        self.logs_dir = config.get("cluster", "cassandra_logs_dir")
        self.data_dir = config.get("cluster", "cassandra_data_dir")

        self.bin_dir = self.base_dir + "/bin"

        # TODO: allow to specify the seeds
        self.seeds = hosts[0]
        self.hosts = hosts

        # Create topology
        logger.info("Cassandra cluster created in hosts %s",
                    ' '.join([style.host(h.address.split('.')[0])
                              for h in self.hosts]))

    def bootstrap(self, tar_file):

        logger.info("Copy " + tar_file + " to hosts and uncompress")
        rm_dirs = TaktukRemote("rm -rf " + self.base_dir +
                               " " + self.conf_dir,
                               self.hosts)
        put_tar = TaktukPut(self.hosts, [tar_file], "/tmp")
        mkdirs = TaktukRemote("mkdir -p " + self.base_dir, self.hosts)
        tar_xf = TaktukRemote(
                "tar xzf /tmp/" + os.path.basename(tar_file) +
                " --strip-components=1 -C " + self.base_dir, self.hosts)
        SequentialActions([rm_dirs, put_tar, mkdirs, tar_xf]).run()

        # 4. Configure start/stop scripts in each peer
        command_start = "cat >> " + self.bin_dir + "/start-cassandra.sh << EOF\n"
        command_start += self.bin_dir + "/cassandra -p " 
        command_start += self.base_dir + "/cassandra.pid\n"
        command_start += "EOF\n"
        action = Remote(command_start, self.hosts)
        action.run()

        logger.info("Create start/stop scripts")
        command_stop = "cat >> " + self.bin_dir + "/stop-cassandra.sh << EOF\n"
        command_stop += "kill \`cat " + self.base_dir + "/cassandra.pid\`\n"
        command_stop += "EOF\n"
        action = Remote(command_stop, self.hosts)
        action.run()

        logger.info("Create the script to adjust the cassandra.yaml file")
        adjust_conf = "cat >> " + self.bin_dir + "/adjust_conf.py << EOF \n"
        adjust_conf += "#!/usr/bin/env python\n"
        adjust_conf += "import sys\n"
        adjust_conf += "from yaml import load, dump, CDumper, Dumper\n"
        adjust_conf += "conf_file = sys.argv[1]\n"
        adjust_conf += "seeds = sys.argv[2]\n"
        adjust_conf += "orig = load(open(conf_file))\n"
        adjust_conf += "orig['seed_provider'][0]['parameters'][0]['seeds'] = seeds\n"
        adjust_conf += "orig['listen_address'] = None\n"
        adjust_conf += "orig['rpc_address'] = None\n"
        adjust_conf += "orig['start_rpc'] = True\n"
        adjust_conf += "orig['endpoint_snitch'] = 'GossipingPropertyFileSnitch'\n"
        adjust_conf += "orig['auto_bootstrap'] = False\n"
        adjust_conf += "new = dump(orig)\n"
        adjust_conf += "out = open(conf_file, 'w')\n"
        adjust_conf += "out.write(new)\n"
        adjust_conf += "out.flush()\n"
        adjust_conf += "out.close()\n"
        adjust_conf += "EOF\n"
        action = Remote(adjust_conf, self.hosts)
        action.run()

        logger.info("Create installation directories")
        mkdirs = TaktukRemote("mkdir -p " + self.conf_dir +
                              " ; mkdir -p " + self.data_dir +
                              " ; mkdir -p " + self.logs_dir,
                              self.hosts)
        chmods = TaktukRemote("chmod g+w " + self.base_dir +
                              " ; chmod g+w " + self.conf_dir +
                              " ; chmod g+w " + self.data_dir +
                              " ; chmod g+w " + self.logs_dir +
                              " ; chmod -R gu+x " + self.bin_dir,
                              self.hosts)
        SequentialActions([mkdirs, chmods]).run()

        initialized = True  # No need to call initialize()

    def initialize(self):
        """Initialize the cluster: copy base configuration."""

        #self._pre_initialize()
        logger.info("Initializing Cassandra")
        self._configure_nodes(self.hosts)
        self.initialized = True

    def _pre_initialize(self):
        """Clean previous configurations"""

        if self.initialized:
            if self.running:
                self.stop()
            self.clean()

        self.initialized = False

    def start(self):
        """
        Start Cassandra processes in all nodes.
        """

        logger.info("Starting Cassandra")

        if self.initialized == False:
            logger.error("Cassandra not inicalized yet! " +
                         "You must initialize it first")
            return
            

        if self.running:
            logger.warn("Cassandra was already started")
            return

        start_seeds = TaktukRemote(self.bin_dir + "/start-cassandra.sh",
                                   [self.seeds])
        start_nodes = TaktukRemote(self.bin_dir + "/start-cassandra.sh",
                                  self.hosts[1:])
        SequentialActions([start_seeds, start_nodes]).run()

        if not start_seeds.finished_ok or not start_nodes.finished_ok:
            logger.warn("Error while starting Cassandra")
            return

        self.running = True


    def stop(self):
        """Stop Cassandra process."""

        logger.info("Stopping Cassandra")

        proc = TaktukRemote(self.bin_dir + "/stop-cassandra.sh",
                            self.hosts)
        proc.run()
        self.running = False

    def clean(self):
        """Remove all files created."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        self.clean_conf()
        self.clean_data()

        # Remove logs might not be good idea, you might want to analyze them
        # self.clean_logs()

        self.initialized = False

    def clean_conf(self):
        """ Clean configuration files in all nodes. """
        rm_conf = Remote("rm -rf " + self.conf_dir + "/cassandra.yaml", self.hosts)
        rm_conf.run()

    def clean_data(self):
        """ Clean data files in all nodes. """
        rm = Remote("rm -rf " + self.data_dir + "/* ", self.hosts)
        rm.run()

    def clean_logs(self):
        """ Clean log files in all nodes. """
        rm_logs = Remote("rm -rf " + self.logs_dir + "/* ", self.hosts)
        rm_logs.run()

    def _configure_nodes(self, hosts=None):
        """Configure nodes and host-dependant parameters.
        """

        if hosts == None:
            hosts = self.hosts


        config_file = self.conf_dir + "/cassandra.yaml"
        parse_yaml = TaktukRemote(self.bin_dir + "/adjust_conf.py " + config_file +
                            " " + self.seeds.address, hosts)
        parse_yaml.run()
