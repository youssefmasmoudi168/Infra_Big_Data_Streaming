$jars = @(
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar",
    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar"
)

$dest = "c:\projets\bigdata-antigravity\spark\jars"

foreach ($url in $jars) {
    $filename = Split-Path $url -Leaf
    $output = Join-Path $dest $filename
    Write-Host "Downloading $filename..."
    Invoke-WebRequest -Uri $url -OutFile $output
    Write-Host "Downloaded $filename"
}
