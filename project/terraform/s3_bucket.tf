provider "aws" {
    region = "REGION" # like eu-central-1
    access_key = "AWS_ACCESS_KEY"
    secret_key = "AWS_SECRET_KEY"
}

resource "aws_s3_bucket" "first" {
    bucket = "real-estate-baku"
}

resource "aws_s3_bucket_acl" "example1" {
    bucket = aws_s3_bucket.first.id
    acl = "private"
}