$groupName = $args[0]
$deviceListFile = $args[1]
try {
    $deviceList = Import-Csv -Path $deviceListFile
    Connect-AzureAD
    $groupObj = Get-AzureADGroup -SearchString $groupName
    foreach ($device in $deviceList) {
        $deviceObj = Get-AzureADDevice -SearchString $device.azureADDeviceId
        if($deviceObj -ne $null){
            try{
                foreach($dev in $deviceObj){
                    if($dev.DeviceId -eq $device.azureADDeviceId){
                        Add-AzureADGroupMember -ObjectId $groupObj.ObjectId -RefObjectId $dev.ObjectId
                    }
                }
            }
            catch{}
        }
        else{
           Write-Host "No device found:$($device.azureADDeviceId)"
        }
    }
}
catch {
    Write-Host -Message $_
}