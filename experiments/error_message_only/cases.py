"""Historical filename prefixes and Table 5.10 case labels."""

# reason, paper type, filename prefix, dataset-1 count, dataset-2 count, original CSV
CASES = [
    ("ClaimLost", "PVLost", "ClaimLost-PV-", 0, 23, None),
    ("Evicted", "LowOnResource", "Evicted-LowOnResource-", 20, 35, "Evicted-LowOnResource-repeat.csv"),
    ("Evicted", "NodeDiskPressure", "Evicted-NodeDiskPressure-", 24, 24, "Evicted-NodeDiskPressure-repeat.csv"),
    ("Failed", "AccessDenied", "Failed-AccessDenied-", 39, 0, "Failed-AccessDenied.csv"),
    ("Failed", "ArtifactNotFound", "Failed-ArtifactNotFound-", 20, 22, "Failed-ArtifactNotFound-repeat.csv"),
    ("Failed", "NetworkUnreachable", "Failed-NetworkUnreachable-", 21, 0, "Failed-NetworkUnreachable-repeat.csv"),
    ("Failed", "NoVolumeToMount", "Failed-NoVolumeToMount-", 24, 37, "Failed-NoVolumeToMount-repeat.csv"),
    ("FailedCreate", "ExceedQuotaJob", "FailedCreate-ExceedQuota-Job-", 45, 46, "FailedCreate-ExceedQuota-Job-2-3.csv"),
    ("FailedCreate", "ExceedQuotaReplicaSet", "FailedCreate-ExceedQuota-ReplicaSet-", 32, 54, "FailedCreate-ExceedQuota-ReplicaSet-2-10.csv"),
    ("FailedCreate", "ExceedQuotaStatefulSet", "FailedCreate-ExceedQuota-StatefulSet-", 20, 44, "FailedCreate-ExceedQuota-StatefulSet-repeat.csv"),
    ("FailedCreate", "ServiceAccountNotFound", "FailedCreate-ServiceAccountNotFound-", 32, 40, "FailedCreate-ServiceAccountNotFound-2-10.csv"),
    ("FailedMount", "ConfigMapNotFound", "FailedMount-ConfigMapNotFound-", 43, 58, "FailedMount-ConfigMapNotFound-2-5.csv"),
    ("FailedMount", "FailedSyncConfigMapCache", "FailedMount-FailedSyncConfigMapCache-", 56, 64, "FailedMount-FailedSyncConfigMapCache-1-1.csv"),
    ("FailedMount", "FailedSyncSecretCache", "FailedMount-FailedSyncSecretCache-", 57, 60, "FailedMount-FailedSyncSecretCache-1-1.csv"),
    ("FailedMount", "NoSuchFileDir", "FailedMount-NoSuchFileDir-", 47, 42, "FailedMount-NoSuchFileDir-2-10.csv"),
    ("FailedMount", "ObjectNotRegistered", "FailedMount-ObjectNotRegistered-", 24, 34, "FailedMount-ObjectNotRegistered-repeat.csv"),
    ("FailedMount", "PVCNotBound", "FailedMount-PVCNotBound-", 0, 56, None),
    ("FailedMount", "SecretNotFound", "FailedMount-SecretNotFound-", 56, 55, "FailedMount-SecretNotFound-2-5.csv"),
    ("FailedMount", "ServiceAccountNotFound", "FailedMount-ServiceAccountNotFound-", 0, 25, None),
    ("FailedMount", "StaleNFS", "FailedMount-StaleNFS-", 21, 0, "FailedMount-StaleNFS-repeat.csv"),
    ("FailedScheduling", "PVCNotFound", "FailedScheduling-PVCNotFound-", 0, 34, None),
    ("FailedScheduling", "UnboundPVC", "FailedScheduling-UnboundPVC-", 38, 71, "FailedScheduling-UnboundPVC-1-1.csv"),
    ("OutOfpods", "NodeNotEnough", "OutOfpods-NodeNotEnough-", 0, 19, None),
]
