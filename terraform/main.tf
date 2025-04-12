terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  #  credentials = 
  project = "global-rookery-448215-m8"
  region  = "us-central1"
}

resource "google_storage_bucket" "bike_data_bucket" {
  name     = "global-rookery-448215-m8_bike_data_raw"
  location = "US"

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 1
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "bike_dataset" {
  dataset_id = "bikesdata_share"
  project    = "global-rookery-448215-m8"
  location   = "US"
}