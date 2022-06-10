from atlasapi.atlas import Atlas
from atlasapi.clusters import ClusterConfig, ClusterType
from atlasapi.specs import ReplicaSetTypes, AtlasPeriods, AtlasGranularities, Host, AtlasMeasurementTypes,\
    AtlasMeasurement, AtlasMeasurementValue
from pprint import pprint
from typing import List, Optional, Generator, Union, Iterable
import humanfriendly as hf



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

    def store_measurement(self, measurements_obj: AtlasMeasurement) -> bool:

        try:
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
            pprint(f'We got an error adding the measurement: {e}')
            return False
        return True

    def store_measurements(self, atlas_obj: Atlas, granularity: AtlasGranularities = AtlasGranularities.FIVE_MINUTE,
                           period = AtlasPeriods.WEEKS_1) -> None:
        status_str = f'Getting Measurements for {len(METRICS)} metrics. . .'
        for each_measurement in METRICS:
            print(status_str, end='\r')
            result = list(self.host_obj.get_measurement_for_host(atlas_obj=atlas_obj,
                granularity=granularity, period= period, measurement=each_measurement))[0]
            self.store_measurement(result)
            status_str.join('.')


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

    def _hosts(self, atlas_obj: Atlas) -> Iterable[Host]:
        if len(list(atlas_obj.Hosts.host_list)) == 0:
            atlas_obj.Hosts.fill_host_list(for_cluster=self.name)

        atlas_obj.Hosts.fill_host_list(for_cluster=self.name)
        return atlas_obj.Hosts.host_list

    def _primary(self, atlas_obj: Atlas) -> Host:
        if len(list(atlas_obj.Hosts.host_list)) == 0:
            atlas_obj.Hosts.fill_host_list(for_cluster=self.name)
        return list(atlas_obj.Hosts.host_list_primaries)[0]

    def hosts(self,atlas_obj: Atlas) -> Iterable[HostData]:
        for each in self._hosts(atlas_obj):
            yield HostData(each)

    def primary_metrics(self, atlas_obj: Atlas,
                        granularity: AtlasGranularities = None, period: AtlasPeriods = None ) -> HostData:
        """Returns Atlas Metrics for the cluster's primary.

        Returns the pre-defined metrics defined in METRICS

        :param atlas_obj: and instantiated Atlas object for connectivity to the API
        :param granularity: The granularity to be used for metrics.
        :param period: The period to be used for metrics.
        :return:
        """
        primary: HostData = HostData(self._primary(atlas_obj=atlas_obj))
        primary.store_measurements(atlas_obj, granularity=granularity,period=period)
        return primary


class Fleet:
    def __init__(self, atlas_obj: Atlas):
        """Holds information for an Atlas Fleet.

        Can be single or Multi orginization.

        :type atlas_obj: Atlas
        """
        self.atlas = atlas_obj

    @property
    def clusters_list(self) -> Iterable[ClusterData]:
        """List of all clusters (databases) in the Fleet.

        if the Atlas obj is instantiated without an Org, this can be from mutiple orgs, depending on the access
        level of the key used.

        """
        for each in self.atlas.Clusters.get_all_clusters(iterable=True):
            cluster = ClusterConfig.fill_from_dict(each)
            cluster_obj = ClusterData(self.atlas.Projects.project_by_id(self.atlas.group).name, self.atlas.group,
                                      cluster.id, cluster.name, cluster.disk_size_gb,
                                      cluster.providerSettings.instance_size_name, cluster.providerSettings.diskIOPS
                                      , cluster.providerSettings.volumeType, cluster.num_shards,
                                      cluster.replication_specs[0].regions_config.get('US_EAST_1').get(
                                          'electableNodes'),
                                      cluster.replication_specs[0].regions_config.get('US_EAST_1').get('analyticsNodes',
                                                                                                       0),
                                      cluster.replication_specs[0].regions_config.get('US_EAST_1').get('readOnlyNodes',
                                                                                                       0),
                                      )
            yield cluster_obj

    def get_full_report_primary_metrics(self,granularity: AtlasGranularities, period: AtlasPeriods) -> Iterable[dict]:
        """

        :type period: object
        :type granularity: AtlasGranularities
        :param granularity: The granularity for the metrics.
        :param period : The period for metrics.
        """
        for each_cluster in self.clusters_list:
            host_data = each_cluster.primary_metrics(atlas_obj=self.atlas,granularity=granularity, period=period)
            base_dict = each_cluster.__dict__
            base_dict[str(host_data.cache_used.name)] = host_data.cache_used.measurement_stats.mean
            base_dict[str(host_data.cache_dirty.name)] = host_data.cache_dirty.measurement_stats.mean
            base_dict[str(host_data.cache_bytes_read.name)] = host_data.cache_bytes_read.measurement_stats.mean
            base_dict[str(host_data.cache_bytes_written.name)] = host_data.cache_bytes_written.measurement_stats.mean

            base_dict[str(host_data.targeting_objects.name)] = host_data.targeting_objects.measurement_stats.mean
            base_dict[str(host_data.targeting_per_returned.name)] = host_data.targeting_per_returned.measurement_stats.mean

            base_dict[str(host_data.queued_readers.name)] = host_data.queued_readers.measurement_stats.mean
            base_dict[str(host_data.queued_writers.name)] = host_data.queued_writers.measurement_stats.mean

            base_dict[str(host_data.tickets_write.name)] = host_data.tickets_write.measurement_stats.mean
            base_dict[str(host_data.tickets_read.name)] = host_data.tickets_read.measurement_stats.mean

            base_dict[str(host_data.db_data_size.name)] = host_data.db_data_size.measurement_stats.mean
            base_dict[str(host_data.db_storage.name)] = host_data.db_storage.measurement_stats.mean

            base_dict[str(host_data.net_in_data.name)] = host_data.net_in_data.measurement_stats.mean
            base_dict[str(host_data.net_out_data.name)] = host_data.net_out_data.measurement_stats.mean
            base_dict['Granularity'] = granularity
            base_dict['Period'] = period
            yield base_dict


