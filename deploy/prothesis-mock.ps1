while ($true) {
    Write-Output "hello"

    if ([Console]::In.Peek() -ge 0) {
        $line = [Console]::In.ReadLine()

        if ($null -ne $line) {
            Write-Output "stdin: $line"
        }
    }

    Start-Sleep -Seconds 2
}