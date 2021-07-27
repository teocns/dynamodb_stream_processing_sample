#!/bin/bash

git add *
git commit -m "Release"
git push

s3_bucket_name="lambda-codebases"
lambda_function_name='dynamodb_crawler_process_stream_processing'
uuid=$(uuidgen)
current_dir=$PWD

rm -rf .temp

# Create a temporary directory
mkdir .temp

# Push all code to .temp
cp -r * .temp/.
rm -rf .temp/.temp

# Include .venv resources
cp -r .venv/lib/python3.8/site-packages/* .temp/.

cd .temp/ && zip -r "../$uuid.zip" .


echo "Done compiling code. Uploading to S3"

zip_file_filepath="$current_dir/$uuid.zip"

s3_filepath=s3://$s3_bucket_name/$uuid.zip

echo $uuid.zip

aws s3 cp $zip_file_filepath $s3_filepath

#exit 1

echo "Done uploading code to S3. Updating lambda code"

# Push code to AWS Lambda
#aws lambda update-function-code --function-name $lambda_function_name --zip-file fileb://$zip_file_filepath
#aws lambda update-function-code --function-name $lambda_function_name --s3-bucket $s3_bucket_name --s3-key $uuid.zip --publish

aws lambda update-function-code \
     --function-name $lambda_function_name \
     --s3-bucket $s3_bucket_name \
     --s3-key $uuid.zip \
     --debug

echo "Done uploading new code to $lamda_function_name"

aws s3 rm $s3_filepath

rm $zip_file_filepath

echo "Removed $zip_file_filepath"

