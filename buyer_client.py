import requests
import json
import time
import random
import pprint
import numpy as np
import string

import start_servers as startup

pp = pprint.PrettyPrinter()


class BuyerClient:
    # 35.224.218.211
    def __init__(self, addrs, debug: bool = False):
        self.username = ""
        self.cart = []
        self.debug = debug
        self.addrs = addrs
        self.headers = {'content-type': 'application/json'}

        self.routes = {
            'create account': self.create_account,
            'login': self.login,
            'logout': self.logout,
            'search items': self.search,
            'add items to cart': self.add_items_to_cart,
            'remove item from cart': self.remove_item_from_cart,
            'clear cart': self.clear_cart,
            'display cart': self.display_cart,
            'make purchase': self.make_purchase,
            # 'provide feedback': self.provide_feedback,
            'get seller rating': self.get_seller_rating_by_id,
            'get purchase history': self.get_purchase_history,
            'exit': None # handled differently due to different args
        }

        self.response_times = []
        self.username = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
        self.random_password = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
    
    def get_addr(self):
        return "http://" + random.choice(self.addrs)

    def create_account(self):
        if self.debug:
            # Get user input
            print("Please provide a username and password.")
            username = input("\nusername: ")
            password = input("password: ")
        else:
            username, password = self.username, self.random_password

        data = json.dumps({
            'username': username,
            'password': password
        })

        start = time.time()
        url = self.get_addr() + '/createAccount'
        response = requests.post(url, headers=self.headers, data=data)
        end = time.time()
        self.response_times.append(end-start)

        print('\n', json.loads(response.text)['status'])

    def login(self):
        """
        Change the local state to reflect that the user
        is logged in.
        """
        if self.debug:
            # Get the username and password
            print("\nPlease provide a username and password.")
            username = input("\nusername: ")
            password = input("password: ")
        else:
            username, password = self.username, self.random_password

        data = json.dumps({
            'username': username,
            'password': password
        })

        start = time.time()
        url = self.get_addr() + '/login'
        response = requests.post(url, headers=self.headers, data=data)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)
        if 'Success' in response_text['status']:
            self.username = username
        
        print('\n', response_text['status'])

    def logout(self):
        if self.username:
            data = json.dumps({'username': self.username})
            url = self.get_addr() + '/logout'

            start = time.time()
            response = requests.post(url, headers=self.headers, data=data)
            end = time.time()
            self.response_times.append(end-start)

            response_text = json.loads(response.text)

            print('\n', response_text['status'])
            if 'Success' in response_text['status']:
                self.username = ""

    # def check_if_logged_in(self) -> bool:
    #     """
    #     Calls the server to check if the user is logged in.
    #     """
    #     data = json.dumps({'username': self.username})
    #     url = self.get_addr() + '/checkIfLoggedIn'

    #     start = time.time()
    #     response = requests.post(url, headers=self.headers, data=data)
    #     end = time.time()
    #     self.response_times.append(end-start)

    #     response_text = json.loads(response.text)
    #     return response_text['is_logged_in']

    def search(self):
        """
        Results in the specified category or matching
        any of the keywords get returned.
        """
        if self.debug:
            print("Please provide the following information.")
            category = int(input("\nCategory (0-9): "))
            keywords = input("Keywords: ").split(',')
        else:
            category = random.choice(range(10))
            keywords = ','.join([self._get_random_string(3) for _ in range(4)])
        
        data = json.dumps({
            'category': category,
            'keywords': keywords
        })
        url = self.get_addr() + '/search'

        start = time.time()
        response = requests.post(url, headers=self.headers, data=data)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)

        if 'Error' in response_text['status']:
            print('\n', response_text['status'])
        elif not response_text['items']:
            print("\nYour search did not return any results.")
        else:
            for item in response_text['items']:
                print("")
                pp.pprint(item)

    def add_items_to_cart(self):
        if self.debug:
            item_id = int(input("\nID for the item you wish to add to your cart: "))
            quantity = int(input("Number of this item you wish to add to your cart: "))
            
        else:
            item_id = random.choice(range(100))
            quantity = random.choice(range(5))

        data = json.dumps({
                'username': self.username,
                'item_id': item_id,
                'quantity': quantity
            })
        url = self.get_addr() + '/addItemsToCart'

        start = time.time()
        response = requests.post(url, headers=self.headers, data=data)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)

        if 'Error' in response_text['status']:
            # No such item, not enough, etc.
            print('\n', response_text['status'])
        else:
            self.cart.append(response_text['items'])
            print("\nThe items have been added to your cart")

    def remove_item_from_cart(self):
        item_id = int(input("\nID for the item you wish to remove from your cart: "))
        quantity = int(input("Number of this item you wish to remove from your cart: "))

        # Find that item in the cart
        item_to_remove = None
        for item in self.cart:
            if item['id'] == item_id:
                item_to_remove = item
                break

        if item_to_remove == None:
            print("\nThe item specified is not in your cart")
        elif item_to_remove['quantity'] < quantity:
            print("\nYou have requested to remove more items than are in your cart")
        elif item_to_remove['quantity'] == quantity:
            self.cart.remove(item_to_remove)
            print("\nThe item has been deleted from your cart")
        else:
            item_to_remove['quantity'] -= quantity
            print(f"\n{quantity} of the specified item have been removed")

    def clear_cart(self):
        self.cart = []
        print("\nYour cart has been cleared.")

    def display_cart(self):
        if not self.cart:
            print("\nYour cart is empty.")
        else:
            for item in self.cart:
                print("")
                pp.pprint(item)

    def get_seller_rating_by_id(self):
        if self.debug:
            seller_id = input("\nPlease provide the ID for the seller whose rating you wish to view.\n")
        else:
            seller_id = str(random.choice(range(1, 11)))

        url = self.get_addr() + '/getSellerRatingByID/' + seller_id

        start = time.time()
        response = requests.get(url)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)

        if 'Error' in response_text['status']:
            print('\n', response_text['status'])
        else:
            tu, td = response_text['thumbs_up'], response_text['thumbs_down']
            print(f"\nSeller with ID {seller_id} has {tu} thumbs up and {td} thumbs down")

    def make_purchase(self):
        if not self.cart:
            print("\nPlease add items to your cart before attempting to make a purchase.")
            return None

        if self.debug:
            print("\nWe need the following payment information:")
            cc_name = input("Name on payment card: ")
            cc_number = input("Card number: ")
            cc_exp = input("Card expiration date: ")
        else:
            cc_name = self._get_random_string(4)
            cc_number = self._get_random_string(16)
            cc_exp = self._get_random_string(5)

        data = json.dumps({
            'cc_name': cc_name,
            'cc_number': cc_number,
            'cc_exp': cc_exp,
            'username': self.username,
            'items': [(item['id'], item['quantity'])
                        for item in self.cart]
        })
        url = self.get_addr() + '/makePurchase'

        start = time.time()
        response = requests.post(url, headers=self.headers, data=data)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)

        print("\n", response_text['status'])
        if 'Error' not in response_text['status']:
            self.clear_cart()

    def get_purchase_history(self):
        url = self.get_addr() + '/getPurchaseHistory/' + self.username

        start = time.time()
        response = requests.get(url)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)

        if 'Error' in response_text['status']:
            print('\n', response_text['status'])
        else:
            print(f"\nYou have purchased {response_text['items_purchased']} items.")


    def _get_route(self, route: str):
        return self.routes[route]

    def _get_random_string(self, n: int) -> str:
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))
            
    def serve(self):
        """
        Runs the server either in an interactive mode for debugging
        or an automated mode for performance testing.
        """
        if self.debug:
            # Run in interactive terminal mode
            while True:
                try:
                    # Get user input
                    actions = list(self.routes.keys())

                    # if not self.username:
                    #     actions.remove('logout')
                    # else:
                    #     actions.remove('login')

                    action = input(f"\nWhat would you like to do?\n{actions}\n")

                    # Check that action is valid
                    if action not in actions:
                        print("\nUnknown action. Please select another.\n")
                        continue

                    # Execute the action specified
                    if action == 'exit':
                        self.logout()
                        exit(130)

                    self.routes[action]()
                except KeyboardInterrupt:
                        # Logout then actually exit
                        self.logout()
                        exit(130)

    def test(self, delay):
        start = time.time()
        # Call functions randomly after logging in
        self.create_account()
        time.sleep(delay)
        self.login()
        time.sleep(delay)

        # Call functions randomly
        functions = [
            self.search, self.add_items_to_cart, self.make_purchase,
            self.get_seller_rating_by_id, self.get_purchase_history,
        ]
        probs = [0.22, 0.22, 0.12, 0.22, 0.22]

        for _ in range(500): # Each function calls check_if_logged_in, which calls server
            np.random.choice(functions, size=1, p=probs)[0]()
            time.sleep(delay)

        self.logout()

        # Get server operations and time
        response = requests.get(self.get_addr() + '/getServerInfo')
        response_text = json.loads(response.text)
        experiment_time = response_text['time'] - start
        n_operations = response_text['n_ops']

        # Compute the average response time
        average_response_time = sum(self.response_times) / len(self.response_times)
        return average_response_time, experiment_time, n_operations

    def testEach(self, method):
        start = time.time()

        method()

        # Get server operations and time
        response = requests.get(self.get_addr() + '/getServerInfo')
        response_text = json.loads(response.text)
        experiment_time = response_text['time'] - start
        n_operations = response_text['n_ops']

        # Compute the average response time
        average_response_time = sum(self.response_times) / len(self.response_times)
        return average_response_time, experiment_time, n_operations


if __name__ == "__main__":
    config = startup.getConfig()
    addrs =[ip + ':' + str(port) for ip, port in zip(config.buyerServer.hosts, config.buyerServer.ports)]
    buyer = BuyerClient(addrs, True)
    buyer.serve()