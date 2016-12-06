function process_metric(pyld)
   payload.metric_set_tag(pyld, 1, "bizz", "bazz")
end

function process_log(pyld)
   payload.log_set_tag(pyld, 1, "bizz", "bazz")
end

function tick(pyld)
end
