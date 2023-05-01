variable "project" {
  description = "Your GCP Project ID"
  default = "main-audio-384108"
}

variable "region" {
  description = "Region for GCP resources. https://cloud.google.com/about/locations"
  default = "US"
  type = string
}

variable "storage_class" {
  description = "Storage class type for the GCS bucket."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data will be written to"
  type = string
  default = "sf_bay_area_tripdata"
}
