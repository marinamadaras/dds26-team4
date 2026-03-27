Write-Host "=== START TEST FLOW ==="

# 1. Create user
$user = Invoke-RestMethod -Method Post -Uri "http://localhost:8000/payment/create_user"
$user_id = $user.user_id
Write-Host "User created: $user_id"

# 5. Add funds
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/payment/add_funds/$user_id/100"
Write-Host "Funds added"

# 2. Create items
$item1 = Invoke-RestMethod -Method Post -Uri "http://localhost:8000/stock/item/create/5"
$item1_id = $item1.item_id
Write-Host "Item1: $item1_id"

$item2 = Invoke-RestMethod -Method Post -Uri "http://localhost:8000/stock/item/create/3"
$item2_id = $item2.item_id
Write-Host "Item2: $item2_id"

# 3. Add stock
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/stock/add/$item1_id/10"
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/stock/add/$item2_id/10"
Write-Host "Stock added"

# 4. Create order
$order = Invoke-RestMethod -Method Post -Uri "http://localhost:8000/orders/create/$user_id"
$order_id = $order.order_id
Write-Host "Order created: $order_id"


# 6. Add items to order
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/orders/addItem/$order_id/$item1_id/1"
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/orders/addItem/$order_id/$item2_id/2"
Write-Host "Items added to order"

# 7. Checkout
Invoke-RestMethod -Method Post -Uri "http://localhost:8000/orders/checkout/$order_id"
Write-Host "Checkout triggered"

Write-Host "=== TEST FLOW DONE ==="
