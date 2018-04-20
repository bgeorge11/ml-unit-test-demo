import module namespace admin = "http://marklogic.com/xdmp/admin"
at "/MarkLogic/admin.xqy";
declare variable $maxCount as xs:int external;
declare variable $templateForestName as xs:string external;
declare variable $forestPrefix as xs:string external;

declare function local:CopyForests($config, $counter as xs:int) {
if($counter le $maxCount) then (
let $new-config := admin:forest-copy($config, xdmp:forest($templateForestName), $forestPrefix || $counter, ())
return local:CopyForests($new-config, ($counter + 1))

) else $config
};
admin:save-configuration(local:CopyForests(admin:get-configuration(), 1))

