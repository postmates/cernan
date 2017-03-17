local demo = require "lib/demo"

function process_metric(pyld)
end

function process_log(pyld)
   payload.log_set_tag(pyld, 1, "bizz", demo.demo())
end

function tick(pyld)
end
