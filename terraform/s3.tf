
resource "aws_s3_bucket" "streamlit_data_bucket" {
  bucket_prefix = "streamlit-data-bucket-"
  # No versioning block

  tags = {
    Name        = "Streamlit Data Test Bucket"
    Environment = "Development"
  }
}

resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket = aws_s3_bucket.streamlit_data_bucket.id

  block_public_acls   = true
  block_public_policy = true
#   ignore_public_acls  = true
#   restrict_public_buckets = true
}


output "bucket_name" {
  value = aws_s3_bucket.streamlit_data_bucket.bucket
}