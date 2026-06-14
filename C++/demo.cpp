#include<bits/stdc++.h>
#include<iostream>

using namespace std;


int main() {
     /* Enter your code here. Read input from STDIN. Print output to STDOUT */   
     int N;
     cin>>N;
     vector<int> v(N);
     for(int i=0;i<N;i++)
     {
         cin>>v[i];
     }
 
     sort(v.begin(),v.end());
     for(auto i : v)
     { 
         cout<<i<<" ";
     }
     return 0;
 }
 

