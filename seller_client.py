import requests
import json
import time
import random
import pprint
import string
import numpy as np

pp = pprint.PrettyPrinter()

class SellerClient:
    # 35.222.138.219
    def __init__(self, server_ip : str = '35.222.138.219:5000', debug: bool = False):
        self.username = ""
        self.debug = debug
        self.base_url = 'http://' + server_ip
        self.headers = {'content-type': 'application/json'}

        self.routes = {
            'create account': self.create_account,
            'login': self.login,
            'logout': self.logout,
            'get seller rating': self.get_seller_rating,
            'sell item': self.sell_item,
            'remove item': self.remove_item,
            'change item price': self.change_item_price,
            'list item': self.list_items,
            'exit': None # handled differently due to different args
        }

        # Attributes for automating the testing
        self.response_times = []
        self.username = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
        self.random_password = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))

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
        url = self.base_url + '/createAccount'
        response = requests.post(url, headers=self.headers, data=data)
        end = time.time()

        if not self.debug:
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
        url = self.base_url + '/login'
        response = requests.post(url, headers=self.headers, data=data)
        end = time.time()

        if not self.debug:
            self.response_times.append(end-start)

        response_text = json.loads(response.text)
        if 'Success' in response_text['status']:
            self.username = username
        
        print('\n', response_text['status'])

    def logout(self):
        if self.username:
            data = json.dumps({'username': self.username})
            url = self.base_url + '/logout'

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
    #     url = self.base_url + '/checkIfLoggedIn'

    #     start = time.time()
    #     response = requests.post(url, headers=self.headers, data=data)
    #     end = time.time()
    #     self.response_times.append(end-start)

    #     response_text = json.loads(response.text)
    #     return response_text['is_logged_in']

    def get_seller_rating(self):
        url = self.base_url + f'/getSellerRating/{self.username}'

        start = time.time()
        response = requests.get(url)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)

        if 'Error' in response_text['status']:
                print("\n", response_text['status']),
        else:
            print(f"\nYou have {response_text['thumbs_up']} thumbs up and {response_text['thumbs_down']} thumbs down.")

    def sell_item(self):
        """
        Gather the attributes needed to list an item
        for sale.
        """
        if self.debug:
            name = input("\nItem name: ")
            category = int(input("Item category: "))
            keywords = input("Item keywords: ")
            condition = input("Item condition: ")
            price = float(input("Item price: "))
            quantity = int(input("Item quantity: "))

            item = {
                'name': name,
                'category': category,
                'keywords': keywords,
                'condition': condition,
                'price': round(price, 2),
                'quantity': quantity,
                'seller': self.username,
                'status': 'For Sale',
                'buyer': None
            }
        else:
            item = {
                'name': self._get_random_string(6),
                'category': random.choice(range(10)),
                'keywords': ','.join([self._get_random_string(3) for _ in range(4)]),
                'condition': random.choice(['New', 'Used']),
                'price': round(random.uniform(0, 100), 2),
                'quantity': random.choice(range(1,6)),
                'seller': self.username,
                'status': 'For Sale',
                'buyer': None
            }

        data = json.dumps(item)
        url = self.base_url + '/sellItem'

        start = time.time()
        response = requests.post(url, data=data, headers=self.headers)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)
        print('\n', response_text['status'])

    def list_items(self):
        url = self.base_url + f'/listItems/{self.username}'

        start = time.time()
        response = requests.get(url)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)

        if 'Error' in response_text['status']:
            print('\n', response_text['status'])
        elif not response_text['items']:
            print("\nYou currently have no items listed for sale")
        else:
            print("\nYou have the following items listed for sale:")
            for item in response_text['items']:
                print("")
                pp.pprint(item)

    def remove_item(self):
        if self.debug:
            id = int(input("\nEnter the ID of the item you would like to remove: "))
            quantity = input("How many of this item do you want to remove? ")
        else:
            id = random.choice(range(1, 1000))
            quantity = random.choice(range(1, 3))

        data = json.dumps({
            'username': self.username,
            'item_id': id,
            'quantity': int(quantity)
        })
        url = self.base_url + '/deleteItem'

        start = time.time()
        response = requests.put(url, headers=self.headers, data=data)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)
        print('\n', response_text['status'])

    def change_item_price(self):
        if self.debug:
            id = int(input("\nEnter the ID of the item whose price you wish to change: "))
            new_price = round(float(input("What shall be the new price? ")), 2)
        else:
            id = random.choice(range(1, 1000))
            new_price = round(random.uniform(0, 100), 2)

        data = json.dumps({
            'username': self.username,
            'item_id': id,
            'new_price': new_price
        })
        url = self.base_url + '/changeItemPrice'

        start = time.time()
        response = requests.put(url, headers=self.headers, data=data)
        end = time.time()
        self.response_times.append(end-start)

        response_text = json.loads(response.text)
        print('\n', response_text['status'])

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
        functions = [self.get_seller_rating, self.sell_item, self.list_items,
                        self.remove_item, self.change_item_price]
        probs = [0.24, 0.04, 0.24, 0.24, 0.24]

        for _ in range(500): # Each function calls check_if_logged_in, which makes RPC
            np.random.choice(functions, size=1, p=probs)[0]()
            time.sleep(delay)

        self.logout()

        # Get server operations and time
        response = requests.get(self.base_url + '/getServerInfo')
        response_text = json.loads(response.text)
        experiment_time = response_text['time'] - start
        n_operations = response_text['n_ops']

        # Compute the average response time
        average_response_time = sum(self.response_times) / len(self.response_times)
        return average_response_time, experiment_time, n_operations


if __name__ == "__main__":
    seller = SellerClient()
    seller.serve()