for ($i = 1; $i -le 10; $i++) {

    $url = "http://localhost:8082/api/producer/send-with-timeout?content=msg-$i"

    $result = Measure-Command {
        Invoke-RestMethod -Method Post -Uri $url | Out-Null
    }

    Write-Host "$($result.TotalMilliseconds) ms"
}