import pytest
import logging
import os
import subprocess
from cassandra import ConsistencyLevel
import shutil
import time

from dtest import Tester
from tools.assertions import  assert_one, assert_all
from ccmlib import common as ccmcommon
import faulthandler
from tools.assertions import assert_invalid, assert_one

since = pytest.mark.since
logger = logging.getLogger(__name__)

VERSION_40 = 'github:apache/cassandra-4.1'


#@pytest.mark.upgrade_test
#@since('4.0', max_version='5.99')
class TestDonwgradeTool(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
             # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down'
            #r'Unknown column compaction_properties during deserialization'
        )

    #@pytest.mark.no_offheap_memtables
    def test_downgrade_table(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)

        cassandra = self.patient_exclusive_cql_connection(node1, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) """)

        session.execute("INSERT INTO ks.cf1(id, val) VALUES (0, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (1, 0)")

        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])

        #cluster.stop()

        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","cf1")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)

        session = self.patient_cql_connection(node1)

        assert_all(session, "SELECT * FROM ks.cf1", [[0, 0], [1, 0]],cl=ConsistencyLevel.ONE, ignore_order=True)



    def test_downgrade_index(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        #cluster.start()

        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)

        cassandra = self.patient_exclusive_cql_connection(node1, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};")
        session.execute("""CREATE TABLE ks.users (user_id TEXT PRIMARY KEY,username TEXT,email TEXT, age INT) """)
        session.execute("CREATE INDEX email_idx ON ks.users (email);")

        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('1', 'alice', 'alice@example.com', 30);")
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('2', 'carol', 'carol@example.com', 35);")

        assert_one(session, "SELECT user_id, username FROM ks.users WHERE email = 'carol@example.com';", ['2', 'carol'])
        

        #cluster.stop()

        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","users")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        mark2 = node2.mark_log()
        
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        #node1.watch_log_for("Index [email_idx] became queryable after successful build", from_mark=mark)
        #node1.watch_log_for("Starting listening for CQL", from_mark=mark, timeout=60)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)
        node1.watch_log_for("became queryable after successful build", from_mark=mark)
        
        node2.watch_log_for("Starting listening for CQL", from_mark=mark2)
        node2.watch_log_for("became queryable after successful build", from_mark=mark2)

        #node2.nodetool("rebuild_index ks users email_idx")

        self.patient_cql_connection(node2)
        session = self.patient_cql_connection(node1)

        node1.nodetool("rebuild_index ks users email_idx")
        node2.nodetool("rebuild_index ks users email_idx")

        assert_one(session, "SELECT user_id, username FROM ks.users WHERE email = 'carol@example.com';", ['2', 'carol'], cl=ConsistencyLevel.ONE)

    def test_downgrade_drop_index(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        #cluster.start()

        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)

        cassandra = self.patient_exclusive_cql_connection(node1, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};")
        session.execute("""CREATE TABLE ks.users (user_id TEXT PRIMARY KEY,username TEXT,email TEXT, age INT) """)
        session.execute("CREATE INDEX email_idx ON ks.users (email);")

        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('1', 'alice', 'alice@example.com', 30);")
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('2', 'carol', 'carol@example.com', 35);")

        assert_one(session, "SELECT user_id, username FROM ks.users WHERE email = 'carol@example.com';", ['2', 'carol'])
       
        session.execute("DROP INDEX ks.email_idx ;") 

        #cluster.stop()

        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","users")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        mark2 = node2.mark_log()
        
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        #node1.watch_log_for("Index [email_idx] became queryable after successful build", from_mark=mark)
        #node1.watch_log_for("Starting listening for CQL", from_mark=mark, timeout=60)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)
        node1.watch_log_for("became queryable after successful build", from_mark=mark)
        
        node2.watch_log_for("Starting listening for CQL", from_mark=mark2)
        node2.watch_log_for("became queryable after successful build", from_mark=mark2)

        #node2.nodetool("rebuild_index ks users email_idx")

        self.patient_cql_connection(node2)
        session = self.patient_cql_connection(node1)

        #node1.nodetool("rebuild_index ks users email_idx")
        #node2.nodetool("rebuild_index ks users email_idx")

        assert_invalid(session, "SELECT user_id, username FROM ks.users WHERE email = 'carol@example.com';")


    def test_downgrade_materialzed_view(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """

        cluster = self.cluster

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")

        cluster.set_configuration_options(values={'materialized_views_enabled': 'true'})

        node1, node2 = cluster.nodelist()

        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        cluster.start(no_wait=True, wait_for_binary_proto=True)

        # mark1_start = node1.mark_log()
        # mark2_start = node2.mark_log()
        
        # node1.watch_log_for("Created default superuser role", from_mark=mark1_start)
        # node2.watch_log_for("Created default superuser role", from_mark=mark2_start)

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)
        self.patient_cql_connection(node2)

        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};")
        session.execute("""CREATE TABLE ks.users (user_id TEXT PRIMARY KEY, username TEXT,email TEXT, age INT); """)

        
        session.execute("""
            CREATE MATERIALIZED VIEW ks.users_by_email AS
            SELECT user_id, username, email, age
            FROM ks.users
            WHERE email IS NOT NULL and user_id IS NOT NULL
            PRIMARY KEY (email, user_id);
        """)
 
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('1', 'alice', 'alice@example.com', 30);")
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('2', 'carol', 'carol@example.com', 35);")
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('3', 'carol', 'karl@example.com', 32);")       
        time.sleep(1); 
        assert_one(session, "SELECT user_id, username FROM ks.users_by_email WHERE email = 'carol@example.com';", ['2', 'carol'])


        for node in cluster.nodelist():
            faulthandler.enable()
            print(node)
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","users")
            self.downgrade(node, VERSION_40, "ks","users_by_email")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")
            #node.set_configuration_options(values={'enable_materialized_views': 'true'})


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        cluster.set_configuration_options(values={'enable_materialized_views': 'true'})
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)

        session = self.patient_cql_connection(node1)

        
        assert_one(session, "SELECT user_id, username FROM ks.users_by_email WHERE email = 'carol@example.com';", ['2', 'carol'])


    def test_downgrade_drop_column(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)

        cassandra = self.patient_exclusive_cql_connection(node1, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) """)

        session.execute("INSERT INTO ks.cf1(id, val) VALUES (0, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (1, 0)")

        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])

        session.execute("ALTER TABLE ks.cf1 DROP val;")

        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1])

        #cluster.stop()

        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","cf1")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)

        session = self.patient_cql_connection(node1)

        assert_all(session, "SELECT * FROM ks.cf1", [[0], [1]],cl=ConsistencyLevel.ONE, ignore_order=True)

    def test_downgrade_add_column(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)

        cassandra = self.patient_exclusive_cql_connection(node1, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) """)

        session.execute("INSERT INTO ks.cf1(id, val) VALUES (0, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (1, 0)")

        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])

        session.execute("ALTER TABLE ks.cf1 ADD col1 text;")

        session.execute("INSERT INTO ks.cf1(id, col1) VALUES (0, 'a')")
        session.execute("INSERT INTO ks.cf1(id, col1) VALUES (1, 'b')")

        assert_one(session, "SELECT id, val, col1  FROM ks.cf1 WHERE id=0", [0, 0, 'a'])
        assert_one(session, "SELECT id, val, col1 FROM ks.cf1 WHERE id=1", [1, 0, 'b'])

        #cluster.stop()

        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","cf1")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)

        session = self.patient_cql_connection(node1)

        assert_all(session, "SELECT id, val, col1 FROM ks.cf1", [[0, 0, 'a'], [1, 0, 'b']],cl=ConsistencyLevel.ONE, ignore_order=True)


    def test_downgrade_modify_metadata_table(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)

        cassandra = self.patient_exclusive_cql_connection(node1, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) """)

        session.execute("INSERT INTO ks.cf1(id, val) VALUES (0, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (1, 0)")

        assert_one(session, "SELECT id, val FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT id, val FROM ks.cf1 WHERE id=1", [1, 0])

        session.execute("ALTER TABLE ks.cf1 WITH comment = 'test comment'")

        #cluster.stop()
        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
        assert 'test comment' == meta.options['comment']

        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","cf1")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)

        session = self.patient_cql_connection(node1)

        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
        assert 'test comment' == meta.options['comment']


    def test_downgrade_change_clustering_name_column(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        ## need to change last casandra.yaml to none

        cluster.populate(2)

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")
            
        cluster.start()

        node1, node2 = cluster.nodelist()

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)

        cassandra = self.patient_exclusive_cql_connection(node1, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        session.execute("""CREATE TABLE ks.cf1 (id int, c1 text, val int, PRIMARY KEY (id, c1)) """)

        session.execute("INSERT INTO ks.cf1(id, c1, val) VALUES (0, 'a1', 0)")
        session.execute("INSERT INTO ks.cf1(id, c1, val) VALUES (1, 'a2', 0)")

        assert_one(session, "SELECT id, val, c1 FROM ks.cf1 WHERE id=0", [0, 0, 'a1'])
        assert_one(session, "SELECT id, val, c1 FROM ks.cf1 WHERE id=1", [1, 0, 'a2'])

        session.execute("ALTER TABLE ks.cf1 RENAME c1 TO r1 ")
        assert_one(session, "SELECT id, r1, val FROM ks.cf1 WHERE id=0", [0, 'a1', 0])


        for node in cluster.nodelist():
            faulthandler.enable()
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","cf1")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
        
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)

        session = self.patient_cql_connection(node1)

        assert_one(session, "SELECT id, r1, val FROM ks.cf1 WHERE id=0", [0, 'a1', 0])

        # session.execute("INSERT INTO ks.test (key, column1, column2, column3, value) VALUES ('foo', 4, 3, 2, 'bar')")
        # session.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        # assert_one(session, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])


    def test_downgrade_drop_materialzed_view(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """

        cluster = self.cluster

        cluster.populate(2)

#        for node in cluster.nodelist():
#            node.set_configuration_options(values={'storage_compatibility_mode': 'NONE'})

        for node in cluster.nodelist():
            self.update_compatibility_mode(node, "NONE")

        cluster.set_configuration_options(values={'materialized_views_enabled': 'true'})
        #cluster.start(jvm_args=["-Dcassandra.storage_compatibility_mode=NONE"])
        cluster.start(no_wait=True, wait_for_binary_proto=True)


        node1, node2 = cluster.nodelist()
        # node1.watch_log_for("Created default superuser role")
        # node2.watch_log_for("Created default superuser role")

        running50 = node1.get_base_cassandra_version() >= 5.0
        assert running50

        session = self.patient_cql_connection(node1)
        self.patient_cql_connection(node2)


        session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};")
        session.execute("""CREATE TABLE ks.users (user_id TEXT PRIMARY KEY, username TEXT,email TEXT, age INT); """)

        
        session.execute("""
            CREATE MATERIALIZED VIEW ks.users_by_email AS
            SELECT user_id, username, email, age
            FROM ks.users
            WHERE email IS NOT NULL and user_id IS NOT NULL
            PRIMARY KEY (email, user_id);
        """)
 
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('1', 'alice', 'alice@example.com', 30);")
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('2', 'carol', 'carol@example.com', 35);")
        session.execute("INSERT INTO ks.users (user_id, username, email, age) VALUES ('3', 'carol', 'karl@example.com', 32);")       
        time.sleep(1) 
        
        assert_one(session, "SELECT user_id, username FROM ks.users_by_email WHERE email = 'carol@example.com';", ['2', 'carol'])

        session.execute("DROP MATERIALIZED VIEW ks.users_by_email;")


        for node in cluster.nodelist():
            faulthandler.enable()
            print(node)
            self.stop_node(node)
            self.downgrade(node, VERSION_40, "ks","users")
#            self.downgrade(node, VERSION_40, "ks","users_by_email")
            self.downgrade_system_keyspaces(node, VERSION_40)
            self._cleanup(node)
            node.set_install_dir(version="4.1.5")
            #node.set_configuration_options(values={'enable_materialized_views': 'true'})


        self.cluster.set_install_dir(version="4.1.5")
        

        mark = node1.mark_log()
#        cluster.set_configuration_options(values={'enable_materialized_views': 'true'})
        self.cluster.start(no_wait=True, wait_for_binary_proto=True)

        node1.watch_log_for("Starting listening for CQL", from_mark=mark)

        session = self.patient_cql_connection(node1)
        
        assert_invalid(session, "SELECT user_id, username FROM ks.users_by_email WHERE email = 'carol@example.com';")


           


    def downgrade_system_keyspaces(self, node, version):
            self.downgrade(node, version, "system_auth","cidr_groups")
            self.downgrade(node, version, "system_auth","cidr_permissions")
            self.downgrade(node, version, "system_auth","identity_to_role")
            self.downgrade(node, version, "system_auth","network_permissions")
            self.downgrade(node, version, "system_auth","resource_role_permissons_index")
            self.downgrade(node, version, "system_auth","role_members")
            self.downgrade(node, version, "system_auth","role_permissions")
            self.downgrade(node, version, "system_auth","roles")
            self.downgrade(node, version, "system_schema","aggregates")
            self.downgrade(node, version, "system_schema","column_masks")
            self.downgrade(node, version, "system_schema","columns")
            self.downgrade(node, version, "system_schema","dropped_columns")
            self.downgrade(node, version, "system_schema","functions")
            self.downgrade(node, version, "system_schema","indexes")
            self.downgrade(node, version, "system_schema","keyspaces")
            self.downgrade(node, version, "system_schema","tables")
            self.downgrade(node, version, "system_schema","triggers")
            self.downgrade(node, version, "system_schema","types")
            self.downgrade(node, version, "system_schema","views")
            self.downgrade(node, version, "system_distributed","parent_repair_history")
            self.downgrade(node, version, "system_distributed","partition_denylist")
            self.downgrade(node, version, "system_distributed","repair_history")
            self.downgrade(node, version, "system_distributed","view_build_status")
            self.downgrade(node, version, "system","IndexInfo")
            self.downgrade(node, version, "system","available_ranges")
            self.downgrade(node, version, "system","available_ranges_v2")
            self.downgrade(node, version, "system","batches")
            self.downgrade(node, version, "system","built_views")
            self.downgrade(node, version, "system","compaction_history")
            self.downgrade(node, version, "system","local")
            self.downgrade(node, version, "system","paxos")
            self.downgrade(node, version, "system","paxos_repair_history")
            self.downgrade(node, version, "system","peer_events")
            self.downgrade(node, version, "system","peer_events_v2")
            self.downgrade(node, version, "system","peers")
            self.downgrade(node, version, "system","peers_v2")
            self.downgrade(node, version, "system","prepared_statements")
            self.downgrade(node, version, "system","repairs")
            self.downgrade(node, version, "system","size_estimates")
            self.downgrade(node, version, "system","sstable_activity")
            self.downgrade(node, version, "system","sstable_activity_v2")
            self.downgrade(node, version, "system","table_estimates")
            self.downgrade(node, version, "system","top_partitions")
            self.downgrade(node, version, "system","transferred_ranges")
            self.downgrade(node, version, "system","transferred_ranges_v2")
            self.downgrade(node, version, "system","view_builds_in_progress")
            self.downgrade(node, version, "system_traces","events")
            self.downgrade(node, version, "system_traces","sessions")

    def downgrade(self, node, tag, keyspace=None, table=None):
        format_args = {'node': node.name, 'tag': tag}
        logger.debug('Downgrading node {node} to nb sstables'.format(**format_args))

        self.update_compatibility_mode(node, "UPGRADING")

        logger.debug('{node} stopped'.format(**format_args))

        #logger.debug('Running sstabledowngrade')

        cdir = node.get_install_dir()
        #print("cdirrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
        #print(cdir)
        env = ccmcommon.make_cassandra_env(cdir, node.get_path())



        data_folder = os.path.join(node.get_path(), 'data')
        os.environ['cassandra_storagedir'] = data_folder

        cmd_args = [node.get_tool('sstabledowngrade'), keyspace, table]
        p = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
        stdout, stderr = p.communicate()
        exit_status = p.returncode
        logger.info('stdout: {out}'.format(out=stdout.decode("utf-8")))
        logger.info('stderr: {err}'.format(err=stderr.decode("utf-8")))
        #print("ahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
        print(stdout.decode("utf-8"))
        print(stdout.decode("utf-8"))

        #assert 0 == exit_status, "Downgrade complete"
        print("================================++++> Downgrade complete")
        
        self.remove_lines_with_substring(os.path.join(node.get_conf_dir(), 'cassandra.yaml'), 'storage_compatibility_mode')


        #node.start(wait_for_binary_proto=True)

    def stop_node(self, node):
        node.flush()
        # drain and shutdown
        #node.drain()
        #node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False, gently=False)

    def set_node_to_current_version(self, node, tag):
        node.set_install_dir(version=to_version)
        node.start(wait_for_binary_proto=True)
        #return node.set_install_dir(version=tag, verbose=True)
        #return node.set_install_dir(install_dir="/Users/anisbenbrahim/Documents/projet/ericsson/downgrade_tool_for_cassandra_5/cassandra4")

    def get_node_versions(self):
        return [n.get_cassandra_version() for n in self.cluster.nodelist()]

    def set_rf2_on_system_auth(self, session):
        """
        Set RF=2 on system_auth and repair
        @param session The session used to alter keyspace
        """
        session.execute("""
            ALTER KEYSPACE system_auth
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};
        """)

        logger.debug("Repairing after altering RF")
        self.cluster.repair()


    def get_session(self, node_idx=0, user=None, password=None):
        """
        Connect with a set of credentials to a given node. Connection is not exclusive to that node.
        @param node_idx Initial node to connect to
        @param user User to connect as
        @param password Password to use
        @return Session as user, to specified node
        """
        node = self.cluster.nodelist()[node_idx]
        session = self.patient_cql_connection(node, user=user, password=password)
        return session

    def remove_lines_with_substring(self, filename, substring):
        # Read the contents of the file
        with open(filename, 'r') as file:
            lines = file.readlines()

        # Filter out lines containing the specified substring
        filtered_lines = [line for line in lines if substring not in line]

        # Write the filtered lines back to the file
        with open(filename, 'w') as file:
            file.writelines(filtered_lines)


    def update_compatibility_mode(self, node, mode):
        cassandra_config_path = os.path.join(node.get_conf_dir(), 'cassandra.yaml')
        with open(cassandra_config_path, 'r') as file:
            lines = file.readlines()
            lines = [line for line in lines if "compatibility_mode" not in line]
        with open(cassandra_config_path, 'w') as snitch_file:
            snitch_file.write("storage_compatibility_mode: " + mode + os.linesep)
            snitch_file.writelines(lines)

    def _cleanup(self, node):
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        shutil.rmtree(commitlog_dir)

