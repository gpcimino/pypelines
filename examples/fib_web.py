from fib import fib
import json
import cherrypy


class FibServer(object):
    @cherrypy.expose
    def fib(self, n):
        n = int(n)
        res = [n, fib(n)]
        return json.dumps(res)


if __name__ == '__main__':
    cherrypy.config.update({'server.socket_port': 12345, 'server.socket_host': '0.0.0.0'})    
    cherrypy.quickstart(FibServer())