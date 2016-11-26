import threading
import os 
from jinja2 import Environment, FileSystemLoader, Template
import cherrypy

template_dir = os.path.join( os.path.dirname( os.path.realpath(__file__)), "html") 
env = Environment(loader=FileSystemLoader(template_dir))

class EmbeddedWebServer(object):
    def __init__(self, dag_root):
        self._dag_root = dag_root

    def _cp_dispatch(self, vpath):
        if vpath[0] == 'query' and len(vpath) > 1:
            cherrypy.request.params['path'] = "-".join(vpath[1:])
            del vpath[1:]
            vpath[0] = "query2"
            #print("vpath=" + str(vpath))
            #print("params: " +  str(cherrypy.request.params))
            return self
        return vpath

    @cherrypy.expose
    def structure(self):
        body = ""
        for node in self._dag_root.traverse_preorder():
            body += ("&nbsp;&nbsp;&nbsp;&nbsp;"*node.depth())+  "* <a href='/query/" + node.dinasty() + "'>" + node.name() + "</a></br>"
        print(body)
        return body

    @cherrypy.expose
    def query2(self, path):
        #print("params in query: " +  str(cherrypy.request.params))
        path = path.replace("-", "/")
        #return "query2: " + path
        node = self._dag_root.query(path)

        template = env.get_template('node.html')
        #template = Template('Hello {{ name }}!')
        html = template.render(node=node)
        return html

def run_web_server(dag_root_node, port=7788):
    threading.Thread(target=_run_web_server, args=(port, dag_root_node, )).start()

def _run_web_server(port, dag_root_node):
    cherrypy.config.update({'server.socket_port': port, 'server.socket_host': '0.0.0.0'})
    cherrypy.quickstart(EmbeddedWebServer(dag_root_node))


def chart():
    #implement: https://developers.google.com/chart/interactive/docs/gallery/wordtree
    pass


# import threading
# import cherrypy

# class WebServer(object):
#     def __init__(self, dag_root):
#         self._dag_root = dag_root

#     @cherrypy.expose
#     def index(self):
#         return "Summary of the object</br><a href='/structure'>Structure</a></br>"

#     @cherrypy.expose
#     def structure(self):
#         body = ""
#         for node in self._dag_root.traverse_preorder():
#             body += ("&nbsp;&nbsp;&nbsp;&nbsp;"*node.depth())+  "* <a href='/structure/" + node.dinasty() + "'>" + node.name() + "</a></br>"
#         print(body)
#         return body

#     @cherrypy.expose
#     def single(self, name):
#         return "OK " + name

#     def _cp_dispatch(self, vpath):
#         print("***********************************************************" + str(vpath))
#         return vpath

# def run_web_server(dag_root_node, port=7788):
#     threading.Thread(target=_run_web_server, args=(port, dag_root_node, )).run()

# def _run_web_server(port, dag_root_node):
#     cherrypy.config.update({'server.socket_port': port, 'server.socket_host': '0.0.0.0'})
#     cherrypy.quickstart(WebServer(dag_root_node))

