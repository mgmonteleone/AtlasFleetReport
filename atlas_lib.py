import datetime

from atlasapi.atlas import Atlas
from atlasapi.clusters import ClusterConfig, ClusterType
from atlasapi.specs import ReplicaSetTypes, AtlasPeriods, AtlasGranularities, Host, AtlasMeasurementTypes, \
    AtlasMeasurement, AtlasMeasurementValue
from pprint import pprint
from typing import List, Optional, Generator, Union, Iterable
from pandas import DataFrame as df
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)
METRICS = [
    AtlasMeasurementTypes.Cache.bytes_read,
    AtlasMeasurementTypes.Cache.bytes_written,
    AtlasMeasurementTypes.Cache.used,
    AtlasMeasurementTypes.Cache.dirty,
    AtlasMeasurementTypes.TicketsAvailable.reads,
    AtlasMeasurementTypes.TicketsAvailable.writes,
    AtlasMeasurementTypes.GlobalLockCurrentQueue.readers,
    AtlasMeasurementTypes.GlobalLockCurrentQueue.writers,
    AtlasMeasurementTypes.Db.storage,
    AtlasMeasurementTypes.Db.data_size,
    AtlasMeasurementTypes.QueryTargetingScanned.per_returned,
    AtlasMeasurementTypes.QueryTargetingScanned.objects_per_returned,
    AtlasMeasurementTypes.Network.bytes_in,
    AtlasMeasurementTypes.Network.bytes_out,
]

DISK_METRICS = [
    AtlasMeasurementTypes.Disk.IOPS.read,
    AtlasMeasurementTypes.Disk.IOPS.read_max,
    AtlasMeasurementTypes.Disk.IOPS.write,
    AtlasMeasurementTypes.Disk.IOPS.write_max,
    AtlasMeasurementTypes.Disk.Latency.write,
    AtlasMeasurementTypes.Disk.Latency.write_max,
    AtlasMeasurementTypes.Disk.Latency.read,
    AtlasMeasurementTypes.Disk.Latency.read_max,
    AtlasMeasurementTypes.Disk.Util.util,
    AtlasMeasurementTypes.Disk.Util.util_max
]


class HostData:
    def __init__(self, host_obj: Host):
        """Holds information for each Atlas Host

        :param host_obj: An atlasAPI host object.
        """
        self.host_obj: Host = host_obj
        self.net_out_data: Optional[AtlasMeasurement] = None
        self.net_in_data: Optional[AtlasMeasurement] = None

        self.cache_bytes_read: Optional[AtlasMeasurement] = None
        self.cache_bytes_written: Optional[AtlasMeasurement] = None
        self.cache_used: Optional[AtlasMeasurement] = None
        self.cache_dirty: Optional[AtlasMeasurement] = None

        self.tickets_read: Optional[AtlasMeasurement] = None
        self.tickets_write: Optional[AtlasMeasurement] = None

        self.queued_readers: Optional[AtlasMeasurement] = None
        self.queued_writers: Optional[AtlasMeasurement] = None

        self.db_data_size: Optional[AtlasMeasurement] = None
        self.db_storage: Optional[AtlasMeasurement] = None

        self.targeting_per_returned: Optional[AtlasMeasurement] = None
        self.targeting_objects: Optional[AtlasMeasurement] = None

        self.disk_iops_read: Optional[AtlasMeasurement] = None
        self.disk_iops_read_max: Optional[AtlasMeasurement] = None
        self.disk_iops_write: Optional[AtlasMeasurement] = None
        self.disk_iops_write_max: Optional[AtlasMeasurement] = None

        self.disk_latency_write: Optional[AtlasMeasurement] = None
        self.disk_latency_write_max: Optional[AtlasMeasurement] = None
        self.disk_latency_read: Optional[AtlasMeasurement] = None
        self.disk_latency_read_max: Optional[AtlasMeasurement] = None

        self.disk_util: Optional[AtlasMeasurement] = None
        self.disk_util_max: Optional[AtlasMeasurement] = None

    def store_measurement(self, measurements_obj: AtlasMeasurement) -> bool:
        try:
            # Disk Metrics
            if measurements_obj.name == AtlasMeasurementTypes.Disk.IOPS.read:
                self.disk_iops_read = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Disk.IOPS.read_max:
                self.disk_iops_read_max = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Disk.IOPS.write:
                self.disk_iops_write = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Disk.IOPS.read_max:
                self.disk_iops_read_max = measurements_obj

            elif measurements_obj.name == AtlasMeasurementTypes.Disk.Latency.read:
                self.disk_latency_read = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Disk.Latency.read_max:
                self.disk_latency_read_max = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Disk.Latency.write:
                self.disk_latency_write = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Disk.Latency.write_max:
                self.disk_latency_write_max = measurements_obj

            elif measurements_obj.name == AtlasMeasurementTypes.Disk.Util.util:
                self.disk_util = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Disk.Util.util_max:
                self.disk_util_max = measurements_obj

            # DB Size and Storage
            if measurements_obj.name == AtlasMeasurementTypes.Db.data_size:
                self.db_data_size = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Db.storage:
                self.db_storage = measurements_obj

            # Targeting
            elif measurements_obj.name == AtlasMeasurementTypes.QueryTargetingScanned.per_returned:
                self.targeting_per_returned = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.QueryTargetingScanned.objects_per_returned:
                self.targeting_objects = measurements_obj

            # Queues
            elif measurements_obj.name == AtlasMeasurementTypes.GlobalLockCurrentQueue.writers:
                self.queued_writers = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.GlobalLockCurrentQueue.readers:
                self.queued_readers = measurements_obj

            # Tickets
            elif measurements_obj.name == AtlasMeasurementTypes.TicketsAvailable.reads:
                self.tickets_read = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.TicketsAvailable.writes:
                self.tickets_write = measurements_obj


            # Network
            elif measurements_obj.name == AtlasMeasurementTypes.Network.bytes_out:
                self.net_out_data = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Network.bytes_in:
                self.net_in_data = measurements_obj
            # Cache
            elif measurements_obj.name == AtlasMeasurementTypes.Cache.bytes_read:
                self.cache_bytes_read = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Cache.bytes_written:
                self.cache_bytes_written = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Cache.used:
                self.cache_used = measurements_obj
            elif measurements_obj.name == AtlasMeasurementTypes.Cache.dirty:
                self.cache_dirty = measurements_obj

        except Exception as e:
            logger.info(f'We got an error adding the measurement: {e}')
            return False
        return True

    def store_host_measurements(self, atlas_obj: Atlas, granularity: AtlasGranularities, period) -> None:
        """Stores host measurements from the API to the HostData object.



        :param atlas_obj (Atlas):
        :param granularity:
        :param period:
        """
        status_str = f'Getting Measurements for {len(METRICS)} metrics. . .'
        # Retrieving and storing Host Metrics
        for each_measurement in METRICS:
            logger.info(status_str)
            result = list(self.host_obj.get_measurement_for_host(atlas_obj=atlas_obj,
                                                                 granularity=granularity, period=period,
                                                                 measurement=each_measurement))[0]
            self.store_measurement(result)
            status_str.swapcase()

    def store_disk_measurements(self, atlas_obj: Atlas, granularity: AtlasGranularities, period) -> None:
        """Stores disk measurements from the API to the HostData object.



        :param atlas_obj (Atlas):
        :param granularity:
        :param period:
        """
        status_str = f'Getting Measurements for {len(METRICS)} metrics. . .'
        # Retrieving and Storing Disk Measurements

        for each_disk_measure in self.host_obj.data_partition_stats(atlas_obj=atlas_obj,
                                                                    granularity=granularity,
                                                                    period=period):
            self.store_measurement(each_disk_measure)


class ClusterData:
    primary_host: Optional[Host] = None

    def __init__(self, project_name: str, project_id: str,
                 id: str, name: str, disk_size: int, tier: str, IOPS: int, io_type: str,
                 shards: int, electable: int, analytics: int, ro: int,
                 ):
        """Holds key data for an Atlas cluster.

        Includes methods for metrics.

        :param project_name:
        :param project_id:
        :param id:
        :param name:
        :param disk_size:
        :param tier:
        :param IOPS:
        :param io_type:
        :param shards:
        :param electable:
        :param analytics:
        :param ro:
        :db_count:
        """
        self.ro = ro
        self.analytics = analytics
        self.electable = electable
        self.shards = shards
        self.io_type = io_type
        self.IOPS = IOPS
        self.tier = tier
        self.disk_size = disk_size
        self.name = name
        self.id = id
        self.project_id = project_id
        self.project_name = project_name

    def hosts(self, atlas_obj: Atlas) -> Iterable[Host]:
        atlas_obj.Hosts.fill_host_list()
        host_list = list(atlas_obj.Hosts.host_list)
        logger.info(f'Cluster: {self.name}')

        filtered = [host for host in host_list if (host.cluster_name_alias == self.name or host.cluster_name == self.name)]
        return filtered

    def primary(self, atlas_obj: Atlas) -> Optional[Host]:
        """Returns a Host object of the CLusters current primary.

        :param atlas_obj:
        :return:
        """
        primary_member = None
        for each in self.hosts(atlas_obj):
            if each.type == ReplicaSetTypes.REPLICA_PRIMARY:
                primary_member = each
            else:
                pass
        return primary_member

    def db_count(self, atlas_obj: Atlas, userland_only: bool = True) -> int:
        """Returns a count of userland databases on the Primary of the cluster.

        :param atlas_obj:
        :param userland_only:
        :return:
        """
        primary = self.primary(atlas_obj)
        count = 0
        if primary:
            for each in primary.get_databases(atlas_obj):
                if each not in ['admin', 'local', 'config']:
                    count += 1
        return count

    def db_item_count(self, atlas_obj: Atlas, measurement_to_count: AtlasMeasurementTypes.Namespaces):
        """Returns the total count of the passed Namespace measurement for all databases.

        Iterates through all userland databases and adds the max value.

        :param atlas_obj:
        :param measurement_to_count:
        """
        primary = self.primary(atlas_obj)
        logger.info(F"Getting counts for {measurement_to_count}")
        item_count = 0
        for each_database in primary.get_databases(atlas_obj=atlas_obj):
            if each_database not in ['admin', 'local', 'config']:
                database_stats = primary.get_measurements_for_database(atlas_obj=atlas_obj, database_name=each_database)
                for each_measurement in database_stats:
                    if each_measurement.name == measurement_to_count:
                        item_count += each_measurement.measurement_stats.max
        return item_count

    def count_collections(self, atlas_obj: Atlas) -> int:
        return self.db_item_count(atlas_obj, AtlasMeasurementTypes.Namespaces.collection_count)

    def count_indexes(self, atlas_obj: Atlas) -> int:
        return self.db_item_count(atlas_obj, AtlasMeasurementTypes.Namespaces.index_count)

    def count_views(self, atlas_obj: Atlas) -> int:
        return self.db_item_count(atlas_obj, AtlasMeasurementTypes.Namespaces.view_count)

    def count_objects(self, atlas_obj: Atlas) -> int:
        """ Number of objects (specifically, documents) in all  userland databases across all collections.

        :param atlas_obj:
        :return: Count
        """
        return self.db_item_count(atlas_obj, AtlasMeasurementTypes.Namespaces.object_count)

    def primary_metrics(self, atlas_obj: Atlas,
                        granularity: AtlasGranularities, period: AtlasPeriods, host_metrics: bool, disk_metrics: bool
                        ) -> Optional[HostData]:
        """Returns Atlas Metrics for the cluster's primary.

        Returns the pre-defined metrics defined in METRICS

        :param disk_metrics:
        :param host_metrics:
        :param atlas_obj: and instantiated Atlas object for connectivity to the API
        :param granularity: The granularity to be used for metrics.
        :param period: The period to be used for metrics.
        :return:
        """
        primary: HostData = HostData(self.primary(atlas_obj=atlas_obj))
        if primary.host_obj:
            print(f'The primary is {primary.host_obj.hostname_alias}')
            print(f"In primary metrics Host metrics: {host_metrics}, disk metrics {disk_metrics}")
            if host_metrics is True:
                primary.store_host_measurements(atlas_obj, granularity=granularity, period=period)
                print("Retrieving Host metrics")
            if disk_metrics is True:
                primary.store_disk_measurements(atlas_obj, granularity=granularity, period=period)
                print("Retrieving disk metrics")
            return primary
        else:
            print(f"Could not find a primary host object")
            return None


class Fleet:
    def __init__(self, atlas_obj: Atlas):
        """Holds information for an Atlas Fleet.



        Can be single org, or api key wide.

        NOTE: If you are using a very widely scoped key (many many orgs, then you may want to instantate with
        a group id to make this more managable.

        :type atlas_obj: Atlas object instantiated at the level desired (key or group)
        """
        self.atlas = atlas_obj

    @property
    def clusters_list(self) -> Iterable[ClusterData]:
        """List of all clusters (databases) in the Fleet.

        if the Atlas obj is instantiated without an Org, this can be from multiple orgs, depending on the access
        level of the key used.

        """
        for each in self.atlas.Clusters.get_all_clusters(iterable=True):
            cluster = ClusterConfig.fill_from_dict(each)
            electable_nodes = 0
            analytics_nodes = 0
            ro_nodes = 0
            try:
                electable_nodes = cluster.replication_specs[0].regions_config.get('US_EAST_1').get(
                                          'electableNodes', 0)
            except:
                pass
            try:
                analytics_nodes = cluster.replication_specs[0].regions_config.get('US_EAST_1').get('analyticsNodes',
                                                                                                       0)
            except:
                pass
            try:
                ro_nodes = cluster.replication_specs[0].regions_config.get('US_EAST_1').get('readOnlyNodes', 0)
            except:
                pass
            cluster_obj = ClusterData(self.atlas.Projects.project_by_id(self.atlas.group).name, self.atlas.group,
                                      cluster.id, cluster.name, cluster.disk_size_gb,
                                      cluster.providerSettings.instance_size_name, cluster.providerSettings.diskIOPS
                                      , cluster.providerSettings.volumeType, cluster.num_shards, electable_nodes,
                                      analytics_nodes, ro_nodes
                                      )
            yield cluster_obj

    def get_full_report_primary_metrics(self, granularity: AtlasGranularities, period: AtlasPeriods,
                                        include_host_metrics: bool,
                                        include_namespace_metrics: bool,
                                        include_disk_metrics: bool) -> Iterable[dict]:
        """

        :param include_disk_metrics:
        :param include_namespace_metrics:
        :param include_host_metrics:
        :type period: object
        :type granularity: AtlasGranularities
        :param granularity: The granularity for the metrics. (default = 10 seconds)
        :param period : The period for metrics. (default = 24 hours)

        """
        if not granularity:
            granularity = AtlasGranularities.TEN_SECOND

        if not period:
            period = AtlasPeriods.HOURS_24
        print(f'Full Report parameters being sent are: G:{granularity}, P:{period}')
        for each_cluster in self.clusters_list:
            try:
                host_data = each_cluster.primary_metrics(atlas_obj=self.atlas, granularity=granularity, period=period,
                                                         host_metrics=include_host_metrics,
                                                         disk_metrics=include_disk_metrics)
                print(f"Host metrics: {include_host_metrics}, disk metrics {include_disk_metrics}")
            except Exception as e:
                logger.debug('--------Error Here-----------')
                raise e
            base_dict = OrderedDict(each_cluster.__dict__)
            try:
                # Namespace Counts
                if include_namespace_metrics:
                    base_dict['views'] = each_cluster.count_views(self.atlas)
                    base_dict['objects'] = each_cluster.count_objects(self.atlas)
                    base_dict['indexes'] = each_cluster.count_indexes(self.atlas)
                    base_dict['collections'] = each_cluster.count_collections(self.atlas)
                    base_dict['databases'] = each_cluster.db_count(self.atlas)

                # Host Measurements
                if include_host_metrics:
                    base_dict[str(host_data.cache_used.name)] = host_data.cache_used.measurement_stats.mean
                    base_dict[str(host_data.cache_dirty.name)] = host_data.cache_dirty.measurement_stats.mean
                    base_dict[str(host_data.cache_bytes_read.name)] = host_data.cache_bytes_read.measurement_stats.mean
                    base_dict[
                        str(host_data.cache_bytes_written.name)] = host_data.cache_bytes_written.measurement_stats.mean

                    base_dict[
                        str(host_data.targeting_objects.name)] = host_data.targeting_objects.measurement_stats.mean
                    base_dict[
                        str(host_data.targeting_per_returned.name)] = host_data.targeting_per_returned.measurement_stats.mean

                    base_dict[str(host_data.queued_readers.name)] = host_data.queued_readers.measurement_stats.mean
                    base_dict[str(host_data.queued_writers.name)] = host_data.queued_writers.measurement_stats.mean

                    base_dict[str(host_data.tickets_write.name)] = host_data.tickets_write.measurement_stats.mean
                    base_dict[str(host_data.tickets_read.name)] = host_data.tickets_read.measurement_stats.mean

                    base_dict[str(host_data.db_data_size.name)] = host_data.db_data_size.measurement_stats.mean
                    base_dict[str(host_data.db_storage.name)] = host_data.db_storage.measurement_stats.mean

                    base_dict[str(host_data.net_in_data.name)] = host_data.net_in_data.measurement_stats.mean
                    base_dict[str(host_data.net_out_data.name)] = host_data.net_out_data.measurement_stats.mean

                # Data Disk Metrics
                if include_disk_metrics:
                    base_dict[str(host_data.disk_util.name)] = host_data.disk_util.measurement_stats.mean
                    base_dict[str(host_data.disk_util_max.name)] = host_data.disk_util_max.measurement_stats.mean

                    base_dict[
                        str(host_data.disk_latency_write.name)] = host_data.disk_latency_write.measurement_stats.mean
                    base_dict[
                        str(host_data.disk_latency_write_max.name)] = host_data.disk_latency_write_max.measurement_stats.mean
                    base_dict[
                        str(host_data.disk_latency_read.name)] = host_data.disk_latency_read.measurement_stats.mean
                    base_dict[
                        str(host_data.disk_latency_read_max.name)] = host_data.disk_latency_read_max.measurement_stats.mean

                    base_dict[str(host_data.disk_iops_read.name)] = host_data.disk_iops_read.measurement_stats.mean
                    base_dict[
                        str(host_data.disk_iops_read_max.name)] = host_data.disk_iops_read_max.measurement_stats.mean
                    base_dict[str(host_data.disk_iops_write.name)] = host_data.disk_iops_write.measurement_stats.mean
                    base_dict[
                        str(host_data.disk_iops_write_max.name)] = host_data.disk_iops_write_max.measurement_stats.mean

                base_dict['Granularity'] = granularity
                base_dict['Period'] = period
            except AttributeError as e:
                # We want to skip over an error which is caused by no primary being available.
                if 'object has no attribute' in str(e):
                    logger.info(f"No primary available for {each_cluster.name}, could not get metrics")
                else:
                    raise e

            yield base_dict

    def get_full_report_primary_metrics_df(self, granularity: AtlasGranularities, period: AtlasPeriods) -> df:
        data_list = []
        for each_record in self.get_full_report_primary_metrics(granularity, period):
            data_list.append(each_record)

        dataFrame = df(data_list)

        return dataFrame

    def events_since(self, since_datetime: datetime):
        event_list = self.atlas.Events.since(since_datetime)
        for each_event in event_list:
            pprint(each_event.__dict__)
