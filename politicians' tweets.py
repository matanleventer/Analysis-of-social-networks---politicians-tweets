import networkx as nx
import community.community_louvain as cl
import json
import datetime as dt
import pandas as pd

def get_name():
    return "Matan Leventer"

def get_id():
    return "208447029"

def community_detector(algorithm_name,network,most_valualble_edge=None):
    if (algorithm_name=='girvin_newman'):
        comp = nx.algorithms.community.girvan_newman(network,most_valualble_edge)
        a=[]
        b=[]
        for i in range(3):
            a.append([])
        for i in comp:
            a[0].append(nx.algorithms.community.quality.modularity(network,i))
            a[1].append(i)
            a[2].append(len(i))
        index= a[0].index(max(a[0]))
        for x in a[1][index]:
            b.append(list(x))
        dict_1={'num_partitions':a[2][index],'modularity':a[0][index],'partition':b}
        return dict_1
    elif (algorithm_name=='louvain'):
        qlice=cl.best_partition(network)
        c=[]
        for i in range(max(qlice.values())+1):
            c.append([])
        for i in qlice.keys():
            c[qlice[i]].append(i)
        dict_1 = {'num_partitions': len(c), 'modularity': cl.modularity(cl.best_partition(network),network), 'partition': c}
        return dict_1
    elif (algorithm_name == 'clique_percolation'):
        f=[]
        for i in range(3):
            f.append([])
        cliques = [list(i) for i in nx.enumerate_all_cliques(network)]
        max_clique_size = max(set(len(lst) for lst in cliques))
        for k in range(2,max_clique_size+1):
            d=nx.algorithms.community.k_clique_communities(network,k)
            e=([list(j) for j in d])
            set_with_com = set(x for lst in e for x in lst)
            set_all = set(list(network.nodes))
            set_without=set_all-set_with_com
            for q in set_without:
               e.append([q])
            f[0].append(Modu(network, e))
            f[1].append([x for x in e if len(x)>1])
            f[2].append(len([x for x in e if len(x)>1]))
        index = f[0].index(max(f[0]))
        dict_1 = {'num_partitions': f[2][index], 'modularity': f[0][index], 'partition': f[1][index]}
        return dict_1

def Modu(G, communities, weight="weight"):
    if not isinstance(communities, list):
        communities = list(communities)
    directed = G.is_directed()
    if directed:
        out_degree = dict(G.out_degree(weight=weight))
        in_degree = dict(G.in_degree(weight=weight))
        m = sum(out_degree.values())
        norm = 1 / m ** 2
    else:
        out_degree = in_degree = dict(G.degree(weight=weight))
        deg_sum = sum(out_degree.values())
        m = deg_sum / 2
        norm = 1 / deg_sum ** 2

    def community_contribution(community):
        comm = set(community)
        L_c = sum(wt for u, v, wt in G.edges(comm, data=weight, default=1) if v in comm)

        out_degree_sum = sum(out_degree[u] for u in comm)
        in_degree_sum = sum(in_degree[u] for u in comm) if directed else out_degree_sum

        return L_c / m - out_degree_sum * in_degree_sum * norm

    return sum(map(community_contribution, communities))

def edge_selector_optimizer(network):
    betweenes = nx.edge_betweenness_centrality(network, weight='weight')
    return max(betweenes, key=betweenes.get)

def construct_heb_edges(files_path,start_date='2019-03-15',end_date='2019-04-15',non_parliamentarians_nodes=0):
    date_start = dt.datetime.strptime(start_date, '%Y-%m-%d')
    date_last = dt.datetime.strptime(end_date, '%Y-%m-%d')
    day_count = (date_last - date_start).days + 1
    users = pd.read_csv(files_path + '/central_political_players.csv')
    users_retweeted = {}
    users_retweeted_importent={}
    users_retweeted_all_1 = {}
    users_retweeted_all_sort_1={}
    list_js=[]
    my_set_node=set()
    list_id_ser=list(users['id'])
    for date in (date_start + dt.timedelta(n) for n in range(day_count)):
        f = open(files_path + '/Hebrew_tweets.json.' + str(date.date()) + '.txt', "r")
        twitts = f.readlines()
        for twit in twitts:
            list_js.append(json.loads(twit))
        for j in list_js:
            try:
                 if(((j['user']['id']) in list_id_ser) and ((j['retweeted_status']['user']['id']) in list_id_ser)):
                    my_set_node.add(j['user']['id'])
                    my_set_node.add(j['retweeted_status']['user']['id'])
                 if (j['user']['id'],j['retweeted_status']['user']['id']) in users_retweeted_importent.keys():
                     users_retweeted_importent[(j['user']['id'],j['retweeted_status']['user']['id'])]+=1
                 else:
                     users_retweeted_importent[(j['user']['id'], j['retweeted_status']['user']['id'])]= 1
            except:
                continue
        f.close()
    if (non_parliamentarians_nodes>0):
        for keys in users_retweeted_importent.keys():
            u=keys[0]
            o=keys[1]
            if o not in users_retweeted_all_1.keys():
                users_retweeted_all_1[o]=users_retweeted_importent[keys]
            else:
                users_retweeted_all_1[o]+=users_retweeted_importent[keys]
            if u not in users_retweeted_all_1.keys():
                users_retweeted_all_1[u]=users_retweeted_importent[keys]
            else:
                users_retweeted_all_1[u]+=users_retweeted_importent[keys]
        sorted_keys_1 = sorted(users_retweeted_all_1, key=users_retweeted_all_1.get, reverse=True)
        if (non_parliamentarians_nodes>0):
            for iten in sorted_keys_1:
                users_retweeted_all_sort_1[iten] = users_retweeted_all_1[iten]
            for rten in users_retweeted_all_sort_1.keys():
                if (non_parliamentarians_nodes == 0):
                    break
                if rten not in list_id_ser:
                    my_set_node.add(rten)
                    non_parliamentarians_nodes -= 1
    for user in users_retweeted_importent.keys():
        if ((user[0] in list_id_ser and user[1] in my_set_node) or (user[1] in list_id_ser and user[0] in my_set_node)):
            if (user[0] not in my_set_node):
                my_set_node.add(user[0])
            if (user[1] not in my_set_node):
                my_set_node.add(user[1])
    for user in users_retweeted_importent.keys():
        if (user[0] in my_set_node and user[1] in my_set_node):
            users_retweeted[user]=users_retweeted_importent[user]
    return users_retweeted


def construct_heb_network(dict_user):
    DG = nx.DiGraph()
    for j in dict_user.keys():
        DG.add_weighted_edges_from([(j[0], j[1], dict_user[j])])
    return DG


