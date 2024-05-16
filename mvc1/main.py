from basic_backend import *
from model_view_controller import *
from mvc_exceptions import *

def main():
    
    my_items = [
        {'name': 'bread', 'price': 0.5, 'quantity': 20},
        {'name': 'milk', 'price': 1.0, 'quantity': 10},
        {'name': 'wine', 'price': 10.0, 'quantity': 5},
    ]
    
    c = Controller(ModelBasic(my_items), View())

    c.show_items() 
    c.show_items(bullet_points=True)
    c.show_item('chocolate') #exc
    c.show_item('bread') 
    c.insert_item('bread', price=1.0, quantity=5) # exc
    c.insert_item('chocolate', price=2.0, quantity=10)
    c.show_item('chocolate')
    c.update_item('milk', price=1.2, quantity=20)
    c.update_item('ice cream', price=3.5, quantity=20) # exc
    c.delete_item('fish') # exc
    c.delete_item('bread')


if __name__ == '__main__':
    main()