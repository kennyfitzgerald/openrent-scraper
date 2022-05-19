# Standard library imports
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import ast

# Local imports
from openrent.configloader import ConfigLoader

class Emailer:
    """ A class that filters a results dataframe and sends an email notification """

    def __init__(self, config_file, df):
        # Create a new instance of the ConfigLoader class
        self.config_loader = ConfigLoader(config_file)
        self.filtered_results = self._filter_results(df)
        self.html = self._create_html(self.filtered_results)

    def _get_item(self, config_item):
        """ Get the value of an item from the config file.
            Calls get_item of ConfigLoader class.

            Args:
                config_item: Name of the item to look up.

            Returns:
                A list of one or more values for the requested item.
        """

        config_item = self.config_loader.get_item(config_item)

        return config_item
        
    def _filter_results(self, df):
        """ Takes dataframe and filters according to specified filters in config """

        df = df.reset_index()
        
        filters = self._get_item('filters')

        filters = ast.literal_eval(filters)

        filters = {k:v for k, v in filters.items() if v is not None}
        columns = [k.replace('max_', '', 1).replace('min_', '', 1).replace('list_', '', 1) for k in filters.keys()]
        operators = ['<=' if k.startswith('max_') else '>=' if k.startswith('min_') else ' in ' if k.startswith('list_') else '==' for k, v in filters.items() ]
        values = [v for k, v in filters.items()]

        query_string = ' & '.join([f'({c} {o} {v})' for c, o, v in zip(columns, operators, values)])

        df = df.convert_dtypes()
        df = df.query(query_string, engine='python')

        return df
    
    def _create_html(self, df):

        results = []
        print(df)
        for row in range(0, len(df)):
            link = f"<p><a href=\"https://openrent.co.uk/{df.iloc[row]['id']}\">{df.iloc[row]['bedrooms']} Bed, {round(df.iloc[row]['rent_per_person'])} PP, {df.iloc[row]['closest_station_mins']} Minutes from {df.iloc[row]['closest_station']} or {df.iloc[row]['second_closest_station_mins']} Minutes from {df.iloc[row]['second_closest_station']}</a></p>"
            results.append(link)

        return '\n'.join(results)

    def send_gmail(self):
        """ Send an email notification through gmail.

            Reads the configuration from email_config.yaml

            Raises:
                SMTPException, if one occured.
        """

        gmail_server = self._get_item('gmail_server')
        gmail_port = self._get_item('gmail_port')
        gmail_user = self._get_item('gmail_user')
        gmail_password = self._get_item('gmail_password')
        gmail_receiver = self._get_item('gmail_receiver')
        gmail_subject = f'Openrent search results'

        msg = MIMEMultipart('alternative')
        msg['From'] = gmail_user
        msg['To'] = gmail_receiver
        msg['Subject'] = gmail_subject

        html_part = MIMEText(self.html, 'html')
        msg.attach(html_part)

        try:
            server = smtplib.SMTP_SSL(gmail_server, gmail_port)
            server.ehlo()
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, gmail_receiver, msg.as_string())
            server.close()
        except:  
            raise