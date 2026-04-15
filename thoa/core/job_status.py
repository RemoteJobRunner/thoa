from enum import StrEnum


class JobStatus(StrEnum):
    CREATED = "created"
    QUEUED = "queued"
    PENDING = "pending"
    UPLOADING = "uploading"
    VALIDATING = "validating"
    STAGING = "staging"
    PROVISIONING = "provisioning"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED_UPLOAD = "failed_upload"
    FAILED_VALIDATION = "failed_validation"
    FAILED_PROVISIONING = "failed_provisioning"
    FAILED_EXECUTION = "failed_execution"
    FAILED_STARTUP = "failed_startup"
    CLEANUP = "cleanup"
    CANCELLED = "cancelled"
    ARCHIVED = "archived"


TERMINAL_STATUSES = {
    JobStatus.COMPLETED,
    JobStatus.FAILED_EXECUTION,
    JobStatus.FAILED_VALIDATION,
    JobStatus.FAILED_PROVISIONING,
    JobStatus.FAILED_UPLOAD,
    JobStatus.FAILED_STARTUP,
    JobStatus.CANCELLED,
}

UPLOAD_STATUSES = {
    JobStatus.CREATED,
    JobStatus.QUEUED,
    JobStatus.PENDING,
    JobStatus.UPLOADING,
}
