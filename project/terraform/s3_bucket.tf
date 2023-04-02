provider "aws" {
    region = "eu-central-1"
    access_key = "AKIA4ADU6SZ5ZPI2AFF2"
    secret_key = "SuAiW6yJLuAaOm37RVnu6jDAZ9jmlIu5SiWb8wMA"
}

resource "aws_s3_bucket" "first" {
    bucket = "real-estate-baku"
}

resource "aws_s3_bucket_acl" "example1" {
    bucket = aws_s3_bucket.first.id
    acl = "private"
}