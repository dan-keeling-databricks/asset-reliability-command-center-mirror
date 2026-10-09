-- Unity Gateway custom policy contract.
-- Only the final work-order submission pauses for native human authorization; evidence reads
-- and proposal preparation remain autonomous. The final SELECT is a deployment smoke test.

CREATE OR REPLACE FUNCTION
  classic_stable_realtime_demos_catalog.asset_reliability_command_center.submit_work_order_policy(
    event VARIANT
  )
RETURNS VARIANT
LANGUAGE SQL
COMMENT 'Require one-time human authorization before submit_work_order executes'
RETURN CASE
  WHEN event:type::string = 'request'
    AND event:context.tool.name::string = 'submit_work_order'
    THEN to_variant_object(named_struct(
      'result', 'ASK',
      'reason', 'Submitting a maintenance work order requires human authorization.'
    ))
  ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
END;

SELECT to_json(
  classic_stable_realtime_demos_catalog.asset_reliability_command_center.submit_work_order_policy(
    parse_json(
      '{"type":"request","context":{"tool":{"name":"submit_work_order"}}}'
    )
  )
) AS policy_contract_check;
