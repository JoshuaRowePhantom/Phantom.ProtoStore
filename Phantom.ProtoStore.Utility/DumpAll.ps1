$Utility = "C:\dev\Phantom.Scalable.Native.TopLevel\out\build\x64-Debug\Phantom.Scalable.Native\Phantom.ProtoStore.Utility\Phantom.ProtoStore.Utility.exe"

Get-Item *.dat, *.dat.deleted |% {
    $_.Fullname | Out-Host
    &$Utility DumpPartition $_.Fullname > "$($_.Fullname).txt"
}
Get-Item *.log, *.log.deleted |% {
    $_.Fullname | Out-Host
    &$Utility DumpLog $_.Fullname > "$($_.Fullname).txt"
}
