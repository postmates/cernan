function process_metric(pyld)
end

function process_log(pyld)
   payload.log_set_value(pyld, 1, "foo")
end

function tick(pyld)
end
