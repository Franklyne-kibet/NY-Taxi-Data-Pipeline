terraform {
    required_version = ">= 1.0"
    backend "local" {}
    required_providers {
        google = {
            source = "hashicorp/google"
        }
    }
}

provider "google" {
    project = var.project
    region = var.region
}

# Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
    name = "ny_taxi_data_lake"
    location = var.region

    #Optional, but recommended settngs:
    storage_class = var.storage_class
    uniform_bucket_level_access  = true

    versioning {
        enabled = false
    }
}

# Data Warehouse
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
    dataset_id = var.BQ_DATASET
    project    = var.project
    location   = var.region
}