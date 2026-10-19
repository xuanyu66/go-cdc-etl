-- Integers and DECIMAL arrive as strings. Never tonumber() a business ID.
local function remove(id)
    return {output="orders", op="delete", key={order_id=id}}
end

function transform(event)
    local old = event.before
    local row = event.after
    if event.op == "delete" then
        return {remove(old.id)}
    end

    local changes = {}
    -- Retire the old output if a primary key changes or a paid order leaves
    -- the selected business state. An empty result alone would leave stale data.
    if event.op == "update" and old.status == "paid" and
       (row.status ~= "paid" or old.id ~= row.id) then
        changes[#changes + 1] = remove(old.id)
    end
    if row.status == "paid" then
        local total = etl.decimal_mul(row.amount, "2")
        changes[#changes + 1] = {
            output="orders",
            op="upsert",
            key={order_id=row.id},
            value={order_id=row.id, customer_id=row.customer_id, total=total}
        }
    end
    return changes
end
