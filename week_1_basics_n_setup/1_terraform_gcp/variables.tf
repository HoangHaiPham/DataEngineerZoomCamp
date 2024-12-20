locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-west3" # Frankfurt region
  type        = string
}

# Not needed for now
variable "bucket_name" {
  description = "The name of the Google Cloud Storage bucket. Must be globally unique."
  default     = ""
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type        = string
  default     = "ny_trips"
}

# Transfer service
variable "access_key_id" {
  description = "AWS access key"
  type        = string
}

variable "aws_secret_key" {
  description = "AWS secret key"
  type        = string
}