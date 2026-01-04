Write-Host "Configuring LocalStack for Kinesis..." -ForegroundColor Green

Get-Content .env | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
    }
}

Write-Host "Waiting for LocalStack to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "Creating Kinesis stream..." -ForegroundColor Cyan
aws kinesis create-stream `
  --stream-name $env:STREAM_NAME `
  --shard-count $env:SHARD_COUNT `
  --endpoint-url $env:LOCALSTACK_ENDPOINT `
  --region $env:AWS_REGION `
  --no-cli-pager

Write-Host "Stream created successfully" -ForegroundColor Green

Write-Host "Verifying stream..." -ForegroundColor Cyan
aws kinesis describe-stream `
  --stream-name $env:STREAM_NAME `
  --endpoint-url $env:LOCALSTACK_ENDPOINT `
  --region $env:AWS_REGION `
  --no-cli-pager

Write-Host "Configuration completed!" -ForegroundColor Green

