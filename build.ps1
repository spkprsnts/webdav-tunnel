$env:GOOS = "linux"; $env:GOARCH = "amd64"; go build -o webdav-tunnel-linux-amd64 .
$env:GOOS = "windows"; $env:GOARCH = "amd64"; go build -o webdav-tunnel-windows-amd64.exe .
Remove-Item Env:GOOS; Remove-Item Env:GOARCH
Write-Host "Done"
Get-ChildItem webdav-tunnel-* | Select-Object Name, @{N='Size';E={"$([math]::Round($_.Length/1MB,1)) MB"}}
