function process_metric(pyld)
   payload.metric_remove_tag(pyld, 1, "bizz")
end

function process_log(pyld)
   payload.log_remove_tag(pyld, 1, "bizz")
end

function tick(pyld)
end
