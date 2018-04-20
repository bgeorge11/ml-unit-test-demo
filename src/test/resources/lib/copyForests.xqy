import module namespace admin = "http://marklogic.com/xdmp/admin"
at "/MarkLogic/admin.xqy";
declare variable $maxCount := 2;
declare variable $templateForestName := "ml9-unit-test-demo-forest1-0";
declare variable $forestPrefix := "ml9-unit-test-demo-forest1-";

declare function local:CopyForests($config, $counter as xs:int) {
if($counter le $maxCount) then (
let $new-config := admin:forest-copy($config, xdmp:forest($templateForestName), $forestPrefix || $counter, ())
return local:CopyForests($new-config, ($counter + 1))

) else $config
};
admin:save-configuration(local:CopyForests(admin:get-configuration(), 1))

