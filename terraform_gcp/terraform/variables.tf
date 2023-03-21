variable "project" {
    description = "de-project-franklyne"
}

variable "region" {
    description = "Region for GCP resources"
    default = "us-central1"
    type = string
}

variable "storage_class" {
    description = "Storage class type for my bucket"
    default = "STANDARD"
}

variable "BQ_DATASET" {
    description = "BigQuery Dataset that raw data (from GCS) will be written to"
    type = string
    default = "trips_data_all"
}